package org.mitchelllisle.reidentifiability

import org.apache.spark.sql.{DataFrame, SparkSession, functions => F}


/**
 * A class to analyze the cardinality of values within a DataFrame using the KHLL algorithm and Spark.
 * This class provides methods to read data, apply the KHLL algorithm, and generate cardinality estimates.
 *
 * @param spark the active SparkSession
 */
class KHyperLogLogAnalyser(spark: SparkSession) {

  private val valueCol = F.col("value")
  private val idCol = F.col("id")

  def createSourceTable(data: DataFrame, columns: Seq[String], primaryKey: String): DataFrame = {
    data.select(
      F.to_json(F.struct(columns.map(F.col): _*)).alias("value"),
      F.to_json(F.struct(F.col(primaryKey))).alias("id")
    )
  }

  def createKHHLTable(data: DataFrame, k: Int = 2048): DataFrame = {
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

  def estimateNumValues(data: DataFrame, k: Int = 2048): DataFrame = {
    val numHashes = data.count()

    data
      .select(
        F.when(F.lit(numHashes) < F.lit(k), F.lit(numHashes))
          .otherwise((F.lit(k - 1) * (F.pow(F.lit(2), F.lit(64)) / (F.max("h") + F.lit(1) + F.pow(F.lit(2), F.lit(63))))).cast("bigint"))
          .alias("estimatedNumValues")
      )
  }

  def apply(data: DataFrame, k: Int = 2048, columns: Seq[String], primaryKey: String): Unit = {
    val source = createSourceTable(data, columns, primaryKey)
    val khll = createKHHLTable(data, k)

  }
}
