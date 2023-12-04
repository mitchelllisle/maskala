package org.mitchelllisle.reidentifiability

import org.apache.spark.sql.{DataFrame, SparkSession, functions => F, Column}

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
      .withColumn("h", F.hash(valueCol))
      .groupBy("h")
      .agg(F.min(F.col("h")).as("h"))
      .orderBy("h")
      .limit(k)

    val idsWithHash = data
      .withColumn("h", F.hash(valueCol))
      .select("h", "id")

    kHashes
      .join(idsWithHash, kHashes("h") === idsWithHash("h"), "left")
      .groupBy("h")
      .agg(F.approx_count_distinct(idCol).as("hll"))
      .orderBy("h")
  }

}
