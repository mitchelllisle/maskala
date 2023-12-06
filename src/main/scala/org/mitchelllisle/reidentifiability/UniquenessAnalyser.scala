package org.mitchelllisle.reidentifiability

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{Column, DataFrame, functions => F}

/**
 * A class to analyze the uniqueness of values within a DataFrame using Spark.
 * This class provides methods to read data from a specified table, calculate uniqueness,
 * and generate uniqueness distribution data.
 *
 * Uniqueness is a proxy for re-identifiability. Knowing how re-identifiable the individuals in a table are
 * is an important privacy engineering concept. This class helps you evaluate some metrics for the risk using the
 * uniqueness of the data as an indicator.
 */
object UniquenessAnalyser {

  private val idCol: Column = F.col("id")
  private val valueCol: Column = F.col("value")
  private val uniquenessCol: Column = F.col("uniqueness")

  /**
   * Calculates the uniqueness of each value in the source table.
   *
   * @param data The DataFrame created from createSourceTable method.
   * @return A DataFrame with each value and its corresponding count of unique user IDs.
   */
  private def createSourceTable(data: DataFrame, groupByColumns: Seq[String], userIdColumn: String): DataFrame = {
    data.select(
      F.sha2(F.to_json(F.struct(groupByColumns.map(F.col): _*)), 256).alias("value"),
      F.to_json(F.struct(userIdColumn)).alias("id")
    )
  }

  /**
   * Calculates the uniqueness of each value in the source table.
   *
   * @param sourceTable The DataFrame created from createSourceTable method.
   * @return A DataFrame with each value and its corresponding count of unique user IDs.
   */
  private def calculateUniquenessData(sourceTable: DataFrame): DataFrame = {
    sourceTable.groupBy(valueCol).agg(F.countDistinct(idCol).alias("uniqueness"))
  }

  /**
   * Counts the total number of unique values in the uniqueness data.
   *
   * @param uniquenessData The DataFrame returned from the calculateUniquenessData method.
   * @return The total number of unique values as a Long.
   */
  private def countNumValues(uniquenessData: DataFrame): Long = {
    uniquenessData.select("value").distinct().count()
  }

  /**
   * Calculates the distribution of values across different levels of uniqueness.
   *
   * @param uniquenessData The DataFrame containing the uniqueness data.
   * @param numValues      The total number of unique values, obtained from countNumValues.
   * @return A DataFrame with the distribution of values for each level of uniqueness.
   */
  private def calculateUniquenessDistribution(uniquenessData: DataFrame, numValues: Long): DataFrame = {
    uniquenessData.groupBy(uniquenessCol)
      .agg(
        F.count(valueCol).alias("value_count"),
        (F.count(valueCol) / F.lit(numValues)).alias("value_ratio")
      )
      .orderBy("uniqueness")
  }

  /**
   * Calculates the cumulative distribution of uniqueness.
   *
   * @param uniquenessDistribution The DataFrame containing the uniqueness distribution.
   * @return A DataFrame with cumulative counts and ratios of the uniqueness distribution.
   */
  private def calculateCumulativeDistribution(uniquenessDistribution: DataFrame): DataFrame = {
    val windowSpec = Window.orderBy("uniqueness").rowsBetween(Window.unboundedPreceding, Window.currentRow)

    uniquenessDistribution
      .withColumn("cumulativeValueCount", F.sum("value_count").over(windowSpec))
      .withColumn("cumulativeValueRatio", F.sum("value_ratio").over(windowSpec))
  }

  /**
   * Orchestrates the process of calculating the uniqueness distribution in a dataset.
   *
   * @param data           The input DataFrame for analysis.
   * @param groupByColumns Columns to be grouped by in the source table.
   * @param userIdColumn   The column representing the user ID.
   * @return The final DataFrame representing the cumulative distribution of uniqueness.
   */
  def apply(data: DataFrame, groupByColumns: Seq[String], userIdColumn: String): DataFrame = {
    // Step 1: Create Source Table
    val sourceTable = createSourceTable(data, groupByColumns, userIdColumn)
    // Step 2: Calculate Uniqueness Data
    val uniquenessData = calculateUniquenessData(sourceTable)
    // Step 3: Count Number of Values
    val numValues = countNumValues(uniquenessData)
    // Step 4: Calculate Uniqueness Distribution
    val uniquenessDistribution = calculateUniquenessDistribution(uniquenessData, numValues)
    // Step 5: Calculate Cumulative Distribution
    calculateCumulativeDistribution(uniquenessDistribution)
  }
}
