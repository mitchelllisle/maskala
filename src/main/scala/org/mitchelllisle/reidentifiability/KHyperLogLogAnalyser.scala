package org.mitchelllisle.reidentifiability

import org.apache.spark.sql.{DataFrame, SparkSession, functions => F, Column}
import org.apache.spark.sql.types.StringType

/**
 * A class to analyze the cardinality of values within a DataFrame using the KHLL algorithm and Spark.
 * This class provides methods to read data, apply the KHLL algorithm, and generate cardinality estimates.
 *
 * @param spark the active SparkSession
 */
class KHyperLogLogAnalyser(spark: SparkSession) {

  val fieldCol: Column = F.col("field")
  val hashCol: Column = F.col("hash")

  /**
   * Reads data from a specified table and returns it as a DataFrame.
   * The data is prepared by hashing the specified columns and casting them to StringType.
   *
   * @param data     the data to hash
   * @param columns    the column names to be hashed
   * @return DataFrame containing the hashed columns
   */
  def hashData(data: DataFrame, columns: Seq[String]): DataFrame = {
    val hashedValue = F.hash(columns.map(F.col): _*).cast(StringType)

    data
      .select(hashedValue)
      .toDF("field")
      .na.drop() // we can't use a null value in KHLL
  }

  /**
   * Applies the KHLL algorithm to estimate the cardinality of the dataset.
   * This method selects the k lowest hashes and aggregates them to estimate uniqueness.
   *
   * @param data the input DataFrame
   * @param k    the number of minimum hashes to consider (hyperparameter of KHLL)
   * @return DataFrame containing the KHLL cardinality estimate
   */
  def khllCardinality(data: DataFrame, k: Int): DataFrame = {
    // Using Spark's approx_count_distinct to estimate cardinality
    val khllEstimate = data
      .withColumn("hash", F.hash(fieldCol))
      .orderBy("hash")
      .limit(k)
      .select(F.approx_count_distinct("hash"))

    khllEstimate
  }

  /**
   * Executes the KHLL analysis workflow.
   * Reads the data, applies KHLL algorithm, and returns cardinality estimates.
   *
   * @param data   the data to compute KHLL
   * @param columns  the column names to be hashed
   * @param k        the number of minimum hashes to consider
   * @return DataFrame containing the cardinality estimates
   */
  def apply(data: DataFrame, columns: Seq[String], k: Int): DataFrame = {
    val hashed = hashData(data, columns)
    khllCardinality(hashed, k)
  }
}
