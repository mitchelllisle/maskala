package org.mitchelllisle.analysers

import org.apache.spark.sql.{Column, DataFrame, functions => F}

case class TClosenessParams(t: Double, sensitiveColumn: String) extends AnalyserParams

/** A class for implementing T-Closeness on DataFrames.
  *
  * T-Closeness is a privacy principle that extends L-Diversity. It requires that the distribution of a sensitive
  * attribute in any equivalence class is close to the distribution of the attribute in the overall dataset. The
  * closeness is measured using the Earth Mover's Distance (EMD), also known as the Wasserstein distance.
  *
  * For categorical attributes, we use a simplified distance metric. For a complete implementation, one should
  * consider the full EMD calculation with a distance matrix between categories.
  *
  * @param t
  *   The maximum allowable distance between the distribution of sensitive values in an equivalence class and the
  *   overall distribution.
  * @param k
  *   The minimum allowable indistinguishable records for K-Anonymity. Default value is 1.
  */
class TCloseness(t: Double, k: Int = 1) extends KAnonymity(k) {

  private def rowHash(groupColumns: Array[Column]): Column = {
    F.sha2(F.concat(groupColumns: _*), 256)
  }

  /** Calculates the total variation distance between the distribution of sensitive values in each equivalence class
    * and the overall distribution.
    *
    * For categorical data, we use the Total Variation Distance (TVD) as an approximation, which is simpler than EMD
    * but provides a reasonable privacy measure.
    *
    * @param data
    *   DataFrame containing the dataset.
    * @param sensitiveColumn
    *   The sensitive column to measure distribution closeness for.
    * @return
    *   A DataFrame with each equivalence class and its distance from the overall distribution.
    */
  def apply(data: DataFrame, sensitiveColumn: String): DataFrame = {
    // Calculate overall distribution
    val totalCount = data.count().toDouble
    val overallDist = data
      .groupBy(sensitiveColumn)
      .agg(F.count("*").as("overall_count"))
      .withColumn("overall_freq", F.col("overall_count") / F.lit(totalCount))
      .select(F.col(sensitiveColumn).as("sens_value"), F.col("overall_freq"))

    // Get equivalence classes (non-sensitive columns)
    val groupColumns: Array[Column] = data.columns.filter(_ != sensitiveColumn).map(F.col)

    // Calculate distribution within each equivalence class
    val equivClassDist = data
      .groupBy((groupColumns :+ F.col(sensitiveColumn)): _*)
      .agg(F.count("*").as("class_count"))
      .withColumn("row_hash", rowHash(groupColumns))

    // Get total count per equivalence class
    val equivClassTotals = equivClassDist
      .groupBy("row_hash")
      .agg(F.sum("class_count").as("total_class_count"))

    // Calculate frequencies within equivalence classes
    val equivClassFreq = equivClassDist
      .join(equivClassTotals, "row_hash")
      .withColumn("class_freq", F.col("class_count") / F.col("total_class_count"))
      .select(
        F.col("row_hash"),
        F.col(sensitiveColumn).as("sens_value"),
        F.col("class_freq"),
        F.col("total_class_count")
      )

    // Join with overall distribution and calculate distance
    val distances = equivClassFreq
      .join(overallDist, equivClassFreq("sens_value") === overallDist("sens_value"), "left_outer")
      .withColumn("overall_freq", F.coalesce(F.col("overall_freq"), F.lit(0.0)))
      .withColumn("freq_diff", F.abs(F.col("class_freq") - F.col("overall_freq")))
      .groupBy("row_hash")
      .agg(
        (F.sum("freq_diff") / F.lit(2.0)).as("distance"), // Total Variation Distance
        F.first("total_class_count").as("count")
      )

    distances
  }

  /** Checks if the DataFrame satisfies the T-Closeness condition.
    *
    * @param data
    *   DataFrame containing the dataset.
    * @param sensitiveColumn
    *   The sensitive column to check t-closeness for.
    * @return
    *   A Boolean indicating if the DataFrame meets T-Closeness.
    */
  def isTClose(data: DataFrame, sensitiveColumn: String): Boolean = {
    val distances = apply(data, sensitiveColumn)
    val maxDistance = distances
      .agg(F.max("distance").as("max_distance"))
      .first()
      .getAs[Double]("max_distance")

    maxDistance <= t
  }

  /** Removes equivalence classes that don't satisfy t-closeness.
    *
    * @param data
    *   DataFrame containing the dataset.
    * @param sensitiveColumn
    *   The sensitive column to check t-closeness for.
    * @return
    *   A DataFrame with only rows that satisfy t-closeness.
    */
  def removeLessThanTRows(data: DataFrame, sensitiveColumn: String): DataFrame = {
    val distances = apply(data, sensitiveColumn)
    val validHashes = distances
      .filter(F.col("distance") <= t)
      .select("row_hash")

    val groupColumns: Array[Column] = data.columns.filter(_ != sensitiveColumn).map(F.col)
    val dataWithHash = data.withColumn("row_hash", rowHash(groupColumns))

    dataWithHash
      .join(validHashes, "row_hash")
      .drop("row_hash")
  }
}
