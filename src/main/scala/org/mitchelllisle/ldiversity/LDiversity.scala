package org.mitchelllisle.ldiversity

import org.apache.spark.sql.{DataFrame, functions => F, Column}
import org.mitchelllisle.kanonymity.KAnonymity

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
  def apply(data: DataFrame, sensitiveColumn: String): DataFrame = {
    val groupColumns: Array[Column] = data.columns.filter(_ != sensitiveColumn).map(data(_))
    data
      .groupBy(groupColumns: _*)
      .agg(F.countDistinct(sensitiveColumn).as("distinctCount"))
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
  def isLDiverse(data: DataFrame, sensitiveColumn: String): Boolean = {
    val distinctCountData = apply(data, sensitiveColumn)
    distinctCountData.filter(F.col("distinctCount") < l).count() == 0
  }

  /** Filters the DataFrame to only include rows where the group of non-sensitive attributes contains at least l
    * distinct sensitive values.
    *
    * @param data
    *   DataFrame containing the dataset.
    * @param sensitiveColumn
    *   The sensitive column that needs to have at least l diverse values.
    * @return
    *   A DataFrame with rows that do not satisfy the l-diversity conditions removed.
    */
  def removeLessThanLRows(data: DataFrame, sensitiveColumn: String): DataFrame = {
    val groupColumns = data.columns.filter(_ != sensitiveColumn)
    val distinctCountData = apply(data, sensitiveColumn)
    val diverseGroups = distinctCountData.filter(F.col("distinctCount") >= l)
    data.join(diverseGroups, groupColumns, "inner").drop("distinctCount")
  }
}
