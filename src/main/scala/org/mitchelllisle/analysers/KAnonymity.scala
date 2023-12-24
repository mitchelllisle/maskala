package org.mitchelllisle.analysers

import org.apache.spark.sql.{DataFrame, functions => F}

/** Represents a class for performing K-anonymity on a DataFrame.
  *
  * @param k
  *   The minimum number of identical rows required for a row to be considered K-anonymous.
  */
class KAnonymity(k: Int) {

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
    data.withColumn("row_hash", F.sha2(F.concat_ws("|", hashCols.map(data(_)): _*), 256))
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
