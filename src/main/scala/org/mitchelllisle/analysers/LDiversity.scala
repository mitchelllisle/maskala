package org.mitchelllisle.analysers

import org.apache.spark.sql.{Column, DataFrame, functions => F}

case class LDiversityParams(l: Int, sensitiveColumn: String, idColumn: String) extends AnalyserParams

/** A class for implementing L-Diversity on DataFrames.
  *
  * L-Diversity is a privacy principle that extends K-Anonymity. It demands that each equivalence class (a set of
  * records that are indistinguishable from each other in terms of released information) has at least l
  * "well-represented" distinct values for the sensitive attributes.
  *
  * @param l
  *   the minimum allowable distinct sensitive values in the equivalence class.
  * @param k
  *   the minimum allowable indistinguishable records for K-Anonymity. Default value is 1.
  */
class LDiversity(l: Int, k: Int = 1) extends KAnonymity(k) {

  private def rowHash(groupColumns: Array[Column]): Column = {
    F.sha2(F.concat(groupColumns: _*), 256)
  }

  /** Group data by non-sensitive columns and count distinct sensitive values in each group.
    *
    * @param data
    *   DataFrame containing the dataset.
    * @param sensitiveColumn
    *   The sensitive column that needs to have at least l diverse values.
    * @return
    *   A DataFrame with grouped data and a new column "distinctCount" representing the count of distinct sensitive
    *   values.
    */
  def apply(data: DataFrame, sensitiveColumn: String, idColumn: String): DataFrame = {
    val groupColumns: Array[Column] = data
      .columns
      .filter(_ != idColumn)
      .filter(_ != sensitiveColumn)
      .map(F.col)
    data
      .groupBy(groupColumns: _*) // We want the resulting DataFrame to have all groupBy columns in the result
      .agg(F.countDistinct(sensitiveColumn).as("distinctCount"))
      .withColumn("row_hash", rowHash(groupColumns)) // this is just here for joining and identifying rows
      .orderBy("distinctCount")
  }

  /** Checks if DataFrame satisfies the L-Diversity condition.
    *
    * @param data
    *   DataFrame containing the dataset.
    * @param sensitiveColumn
    *   The sensitive column that needs to have at least l diverse values.
    * @return
    *   A Boolean indicating if the DataFrame meets L-Diversity.
    */
  def isLDiverse(data: DataFrame, sensitiveColumn: String, idColumn: String): Boolean = {
    val distinctCountData = apply(data, sensitiveColumn, idColumn)
    distinctCountData.filter(F.col("distinctCount") < l).count() == 0
  }
}
