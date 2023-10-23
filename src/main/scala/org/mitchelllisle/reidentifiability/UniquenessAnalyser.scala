package org.mitchelllisle.reidentifiability

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{Column, DataFrame, SparkSession, functions => F}

/**
 * A class to analyze the uniqueness of values within a DataFrame using Spark.
 * This class provides methods to read data from a specified table, calculate uniqueness,
 * and generate uniqueness distribution data.
 *
 * Uniqueness is a proxy for re-identifiability. Knowing how re-identifiable the individuals in a table are
 * is an important privacy engineering concept. This class helps you evaluate some metrics for the risk using the
 * uniqueness of the data as an indicator.
 *
 * @param spark the active SparkSession
 */
class UniquenessAnalyser(spark: SparkSession) {

  val fieldCol: Column = F.col("field")
  val idCol: Column = F.col("id")

  private val uniquenessCol: Column = F.col("uniqueness")

  /**
   * Reads data from a specified table and returns it as a DataFrame.
   * The data that we work on has to have an easy contract since data comes in all sorts of shapes and sizes. The
   * simple contract for any data we bring into this class is by concatenating the specified columns, casting them to
   * StringType, and renaming the field to "field" with an associated "id".
   *
   * @param schema     the schema name of the table
   * @param table      the table name
   * @param primaryKey the primary key column name
   * @param columns    the column names to be concatenated
   * @return DataFrame containing the concatenated columns
   */
  def getTable(schema: String, table: String, primaryKey: String, columns: Seq[String]): DataFrame = {
    val value = F.concat(columns.map(F.col): _*).cast(StringType)
    val id = F.col(primaryKey).cast(StringType)

    spark.read
      .table(s"$schema.$table")
      .select(value, id)
      .toDF("field", "id")
      .na.drop() // we can't hash a null value
  }

  /**
   * Calculates uniqueness data for a DataFrame.
   * Uniqueness is calculated as the count of distinct ids per field value.
   *
   * @param data the input DataFrame
   * @return DataFrame containing the uniqueness data
   */
  def uniquenessData(data: DataFrame): DataFrame = {
    data.groupBy(fieldCol).agg(F.countDistinct(idCol).as("uniqueness"))
  }

  /**
   * Generates a uniqueness distribution for a DataFrame.
   * Groups by uniqueness, calculates value count and value ratio, and orders by uniqueness.
   *
   * @param data       the input DataFrame
   * @param totalCount the total count of values
   * @return DataFrame containing the uniqueness distribution data
   */
  def uniquenessDistribution(data: DataFrame, totalCount: Long): DataFrame = {
    data
      .groupBy(uniquenessCol)
      .agg(
        F.count(fieldCol).as("valueCount"),
        (F.count(fieldCol) / totalCount).as("valueRatio")
      )
      .orderBy(uniquenessCol)
  }

  /**
   * Executes the uniqueness analysis workflow.
   * Computes uniqueness data, gets total value count, calculates uniqueness distribution,
   * and computes cumulative value count and ratio.
   *
   * @param data the input DataFrame
   * @return DataFrame containing the cumulative value count and ratio
   */
  def run(data: DataFrame): DataFrame = {
    val unique = uniquenessData(data)

    val distribution = uniquenessDistribution(unique, unique.count())

    val windowSpec = Window.orderBy("uniqueness").rowsBetween(Window.unboundedPreceding, Window.currentRow)

    distribution
      .withColumn("cumulative_value_count", F.sum("valueCount").over(windowSpec))
      .withColumn("cumulative_value_ratio", F.sum("valueRatio").over(windowSpec))
  }
}
