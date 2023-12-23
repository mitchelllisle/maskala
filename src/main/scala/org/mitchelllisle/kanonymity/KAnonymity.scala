package org.mitchelllisle.kanonymity

import org.apache.spark.sql.{DataFrame, functions => F}

import java.security.MessageDigest

/** Represents a class for performing K-anonymity on a DataFrame.
  *
  * @param k
  *   The minimum number of identical rows required for a row to be considered K-anonymous.
  */
class KAnonymity(k: Int) {
  private val hashUdf = F.udf[String, String](hashRow)

  private def hashRow(s: String): String =
    MessageDigest
      .getInstance("SHA-256")
      .digest(s.getBytes("UTF-8"))
      .map("%02x".format(_))
      .mkString

  /** Retrieves the columns to be used for hash computation.
    *
    * @param data
    *   The DataFrame to extract the columns from.
    * @param columns
    *   Optional parameter specifying the columns to be used for hash computation. If not provided, all columns of the
    *   DataFrame will be used.
    * @return
    *   An array of strings representing the columns to be used for hash computation.
    */
  private def getHashColumns(data: DataFrame, columns: Option[Array[String]]): Array[String] =
    columns.getOrElse(data.columns)

  /** Generates a hashed version of the given DataFrame.
    *
    * @param data
    *   The DataFrame to be hashed.
    * @param columns
    *   Optional array of column names to be included in the hash. If not provided, all columns will be included.
    * @return
    *   A new DataFrame with an additional column "row_hash" that contains the hash value of the specified columns.
    */
  private def getHashedData(data: DataFrame, columns: Option[Array[String]] = None): DataFrame = {
    val hashCols = getHashColumns(data, columns)
    data.withColumn("row_hash", hashUdf(F.concat_ws("|", hashCols.map(data(_)): _*)))
  }

  /** Remove rows from DataFrame that have a count less than or equal to a given threshold.
    *
    * @param data
    *   the DataFrame to remove rows from
    * @param columns
    *   optional array of column names to consider when computing row counts defaults to None, indicating all columns
    *   should be considered
    * @return
    *   a new DataFrame with rows removed if their count is less than the threshold
    */
  def removeLessThanKRows(data: DataFrame, columns: Option[Array[String]] = None): DataFrame = {
    val countedData = apply(data, columns)
    val columnsToHash = getHashColumns(data, columns)
    val hashedData = data.withColumn("row_hash", hashUdf(F.concat_ws("|", columnsToHash.map(data(_)): _*)))

    hashedData
      .join(countedData, "row_hash")
      .filter(F.col("count") >= k)
      .drop("count", "row_hash")
  }

  /** Checks if the given DataFrame is k-anonymous.
    *
    * @param data
    *   The DataFrame to check for k-anonymity.
    * @param columns
    *   Optional array of column names to consider for k-anonymity. If not provided, all columns will be considered.
    * @return
    *   true if the DataFrame is k-anonymous, false otherwise.
    */
  def isKAnonymous(data: DataFrame, columns: Option[Array[String]] = None): Boolean = {
    val kData = apply(data, columns)
    val minCount = kData
      .agg(F.min("count").as("min"))
      .first()
      .getAs[Long]("min")

    minCount >= k
  }

  /** Applies the hashing transformation to the given DataFrame and optionally groups it by hashed rows.
    *
    * @param data
    *   The input DataFrame to be hashed.
    * @param columns
    *   The optional array of column names to be used for hashing. If not provided, all columns will be used.
    * @return
    *   The transformed DataFrame with hashed data.
    */
  def apply(data: DataFrame, columns: Option[Array[String]] = None): DataFrame = {
    val hashedData = getHashedData(data, columns)
    hashedData
      .groupBy(F.col("row_hash"))
      .agg(F.count("*").as("count"))
  }
}
