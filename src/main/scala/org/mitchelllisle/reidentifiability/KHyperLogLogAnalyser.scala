package org.mitchelllisle.reidentifiability

import org.apache.spark.sql.{DataFrame, functions => F}
import org.apache.spark.sql.expressions.Window

/**
 * KHyperLogLogAnalyser is a class to analyze the cardinality of values within a DataFrame using the KHLL algorithm and
 * Spark. This class is inspired by and adapted from the original concept by Google. You can find the implementation in
 * SQL here https://github.com/google/khll-paper-experiments
 *
 */
object KHyperLogLogAnalyser {

  private val valueCol = F.col("value")
  private val idCol = F.col("id")

  /**
   * Estimates the number of unique values in the dataset based on the KHLL algorithm.
   *
   * @param data The DataFrame containing the data to be analyzed.
   * @param k    The number of minimum hashes to consider, default is 2048.
   * @return DataFrame with a single column "estimatedNumValues" containing the estimated number of unique values.
   */
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

  /**
   * Prepares the source table by transforming specified columns into JSON format.
   *
   * @param data       The original DataFrame.
   * @param columns    The columns to be transformed and concatenated into a JSON string.
   * @param primaryKey The primary key column name.
   * @return Transformed DataFrame with "value" and "id" columns in JSON format.
   */
  def createSourceTable(data: DataFrame, columns: Seq[String], primaryKey: String): DataFrame = {
    data.select(
      F.to_json(F.struct(columns.map(F.col): _*)).alias("value"),
      F.to_json(F.struct(F.col(primaryKey))).alias("id")
    )
  }

  /**
   * Creates the KHLL table by calculating the k lowest hashes and aggregating using HyperLogLog.
   *
   * @param data The DataFrame to be processed.
   * @param k    The number of minimum hashes to consider, default is 2048.
   * @return KHLL DataFrame with hash values and HyperLogLog estimates.
   */
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

  /**
   * Calculates the value sampling ratio, which is the estimated ratio of values captured in the sketch.
   *
   * @param data The DataFrame containing the estimated number of unique values.
   * @param k    The number of minimum hashes, default is 2048.
   * @return DataFrame with a single column "valueSamplingRatio" representing the sampling ratio.
   */
  def calculateValueSamplingRatio(data: DataFrame, k: Int = 2048): DataFrame = {
    data.select(
      F.least(F.lit(1.0), F.lit(k) / F.col("estimatedNumValues")).alias("valueSamplingRatio")
    )
  }

  /**
   * Calculates the estimated uniqueness distribution across different levels of uniqueness.
   *
   * @param khllTable               The KHLL DataFrame.
   * @param valueSamplingRatioTable The DataFrame containing the value sampling ratio.
   * @return DataFrame with the distribution of estimated value counts and ratios for each level of uniqueness.
   */
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

  /**
   * Calculates the cumulative distribution of uniqueness, adding up the counts and ratios up to each level of uniqueness.
   *
   * @param estimatedUniquenessDistribution DataFrame containing the estimated uniqueness distribution.
   * @return DataFrame with cumulative counts and ratios of uniqueness.
   */
  def calculateCumulativeDistribution(estimatedUniquenessDistribution: DataFrame): DataFrame = {
    val windowSpec = Window.orderBy("hll").rowsBetween(Window.unboundedPreceding, Window.currentRow)

    estimatedUniquenessDistribution
      .withColumn("cumulativeValueCount", F.sum("estimatedValueCount").over(windowSpec))
      .withColumn("cumulativeValueRatio", F.sum("estimatedValueRatio").over(windowSpec))
  }

  /**
   * Orchestrates the KHLL analysis by applying all steps in sequence and returning the final DataFrame.
   *
   * @param dataFrame      The original DataFrame to be analyzed.
   * @param groupByColumns Columns used for grouping in the source table creation.
   * @param userIdColumn   User ID column name for source table creation.
   * @param k              The number of minimum hashes, default is 2048.
   * @return The final DataFrame representing the cumulative distribution of uniqueness.
   */
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
