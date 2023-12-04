package org.mitchelllisle.reidentifiability

import org.apache.spark.sql.{DataFrame, SparkSession, functions => F}
import org.apache.spark.sql.expressions.Window

/**
 * A class to analyze the cardinality of values within a DataFrame using the KHLL algorithm and Spark.
 * This class provides methods to read data, apply the KHLL algorithm, and generate cardinality estimates.
 *
 * @param spark the active SparkSession
 */
class KHyperLogLogAnalyser(spark: SparkSession) {

  private val valueCol = F.col("value")
  private val idCol = F.col("id")

  def estimateNumValues(data: DataFrame, numHashes: Long, k: Int = 2048): DataFrame = {
    val distinctLargerThanKLogic = (
      F.lit(k - 1) *
        (
          F.pow(F.lit(2),
            F.lit(64)) / (
            F.max("h")
              + F.lit(1)
              + F.pow(F.lit(2), F.lit(63))
            )
          )
      )
      .cast("bigint")

    data
      .select(
        F.when(F.lit(numHashes) < F.lit(k), F.lit(numHashes))
          .otherwise(distinctLargerThanKLogic)
          .alias("estimatedNumValues")
      )
  }

  def createSourceTable(data: DataFrame, columns: Seq[String], primaryKey: String): DataFrame = {
    data.select(
      F.to_json(F.struct(columns.map(F.col): _*)).alias("value"),
      F.to_json(F.struct(F.col(primaryKey))).alias("id")
    )
  }

  def createKHLLTable(data: DataFrame, k: Int = 2048): DataFrame = {
    val kHashes = data
      .withColumn("valueHash", F.hash(valueCol))
      .groupBy("valueHash")
      .agg(F.min(F.col("valueHash")).alias("h"))
      .orderBy("h")
      .select("h")
      .limit(k)

    val idsWithHash = data
      .withColumn("h", F.hash(valueCol))
      .select("h", "id")

    kHashes
      .join(idsWithHash, kHashes("h") === idsWithHash("h"), "left")
      .groupBy(kHashes("h"))
      .agg(F.approx_count_distinct(idCol).as("hll"))
      .orderBy(kHashes("h"))
  }

  def calculateValueSamplingRatio(data: DataFrame, k: Int = 2048): DataFrame = {
    data.select(
      F.least(F.lit(1.0), F.lit(k) / F.col("estimatedNumValues")).alias("valueSamplingRatio")
    )
  }

  def calculateEstimatedUniquenessDistribution(khllTable: DataFrame, valueSamplingRatioTable: DataFrame, numHashes: Long): DataFrame = {
    val valueSamplingRatio = valueSamplingRatioTable.collect()(0).getAs[Double]("valueSamplingRatio")

    khllTable
      .groupBy("hll")
      .agg(
        (F.count("*") / F.lit(valueSamplingRatio)).alias("estimatedValueCount"),
        (F.count("*") / numHashes).alias("estimatedValueRatio")
      )
      .orderBy("hll")
  }

  def calculateCumulativeDistribution(estimatedUniquenessDistribution: DataFrame): DataFrame = {
    val windowSpec = Window.orderBy("hll").rowsBetween(Window.unboundedPreceding, Window.currentRow)

    estimatedUniquenessDistribution
      .withColumn("cumulativeValueCount", F.sum("estimatedValueCount").over(windowSpec))
      .withColumn("cumulativeValueRatio", F.sum("estimatedValueRatio").over(windowSpec))
  }

  def apply(dataFrame: DataFrame, groupByColumns: Seq[String], userIdColumn: String, k: Int = 2048): DataFrame = {
    // Step 1: Create Source Table
    val sourceTable = createSourceTable(dataFrame, groupByColumns, userIdColumn)

    // Step 2: Apply KHLL to Source Table
    val khllTable = createKHLLTable(sourceTable, k)

    val numHashes = khllTable.count()

    // Step 4: Estimate Number of Values
    val estimatedNumValues = estimateNumValues(khllTable, numHashes, k).cache()

    // Step 5: Calculate Value Sampling Ratio
    val valueSamplingRatio = calculateValueSamplingRatio(estimatedNumValues, k)

    // Step 6: Estimate Uniqueness Distribution
    val estimatedUniquenessDistribution = calculateEstimatedUniquenessDistribution(khllTable, valueSamplingRatio, numHashes)

    // Step 7: Calculate Cumulative Distribution
    calculateCumulativeDistribution(estimatedUniquenessDistribution)
  }
}
