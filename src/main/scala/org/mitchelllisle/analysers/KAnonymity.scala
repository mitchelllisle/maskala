package org.mitchelllisle.analysers

import org.apache.spark.sql.{DataFrame, functions => F}

case class KAnonymityParams(k: Int, idColumn: Option[String]) extends AnalyserParams

/** Represents a class for performing K-anonymity on a DataFrame.
  *
  * @param k
  *   The minimum number of identical rows required for a row to be considered K-anonymous.
  */
class KAnonymity(k: Int) {
  /** Generates a hashed version of the given DataFrame.
    *
    * @param data
    *   The DataFrame to be hashed.
    * @param columns
    *   Optional array of column names to be included in the hash. If not provided, all columns will be included.
    * @return
    *   A new DataFrame with an additional column "row_hash" that contains the hash value of the specified columns.
    */
  private def getHashedData(data: DataFrame, columns: Array[String]): DataFrame = {
    data.withColumn("row_hash", F.sha2(F.concat_ws("|", columns.map(data(_)): _*), 256))
  }

  /** Checks if the given DataFrame is k-anonymous.
    *
    * @param data
    *   The DataFrame to check for k-anonymity.
    * @param idColumn
    *  The column that uniquely identifies an individual, if it exists in the dataframe*
    * @return
    *   true if the DataFrame is k-anonymous, false otherwise.
    */
  def isKAnonymous(data: DataFrame, idColumn: Option[String] = None): Boolean = {
    val kData = apply(data, idColumn)
    val minCount = kData
      .agg(F.min("count").as("min"))
      .first()
      .getAs[Long]("min")

    minCount >= k
  }

  def removeLessThanKRows(data: DataFrame, idColumn: Option[String] = None): DataFrame = {
    val kData = apply(data, idColumn)
    kData.filter(F.col("count") >= k)
  }

  /** Applies the hashing transformation to the given DataFrame and optionally groups it by hashed rows.
    *
    * @param data
    *   The input DataFrame to be hashed.
    * @param idColumn
    *  The column that uniquely identifies an individual, if it exists in the dataframe
    * @return
    *   The transformed DataFrame with hashed data.
    */
  def apply(data: DataFrame, idColumn: Option[String] = None): DataFrame = {
    val columns = data.columns.filterNot(_ == idColumn.getOrElse(""))
    val hashedData = getHashedData(data, columns)

    // below we want to group by all columns including our derived row_hash column so that the output contains
    // both the original columns and the row_hash column.
    val columnsWithRowHash = columns ++ Array("row_hash")
    hashedData
      .groupBy(columnsWithRowHash.map(F.col): _*)
      .agg(F.count("*").as("count"))
  }
}
