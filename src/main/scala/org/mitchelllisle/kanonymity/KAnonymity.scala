package org.mitchelllisle.kanonymity

import org.apache.spark.sql.{DataFrame, functions => F}

import java.security.MessageDigest

class KAnonymity(k: Int) {

  private val hashUdf = F.udf[String, String](hashRow)

  /**
   * Generates a SHA-256 hash for a given string.
   *
   * @param s The input string to be hashed.
   * @return A string representing the SHA-256 hash of the input.
   */
  private def hashRow(s: String): String =
    MessageDigest.getInstance("SHA-256")
      .digest(s.getBytes("UTF-8"))
      .map("%02x".format(_))
      .mkString

  /**
   * Adds a hash column to the dataframe by considering specific columns.
   *
   * @param data      The dataframe to which the hash column will be added.
   * @param columns   The columns to consider for hashing. If None assume all columns in `data`
   * @return A new dataframe with an additional "row_hash" column.
   */
  private def getHashedData(data: DataFrame, columns: Option[Array[String]] = None): DataFrame = {
    val hashCols: Array[String] = columns match {
      case None => data.columns
      case _ => columns.get
    }
    data.withColumn("row_hash", hashUdf(F.concat_ws("|", hashCols.map(data(_)): _*)))
  }

  /**
   * Filters the dataframe to only include rows whose frequencies meet a minimum threshold (k).
   *
   * @param data          The dataframe to be filtered.
   * @return A dataframe with rows that meet the minimum frequency threshold.
   */
  def removeLessThanKRows(data: DataFrame, columns: Option[Array[String]] = None): DataFrame = {
    val countsDf = apply(data, columns)
    val hashedData = getHashedData(data, columns)

    hashedData
      .join(countsDf, "row_hash")
      .filter(F.col("count") >= k)
      .drop("count", "row_hash")
  }

  def isKAnonymous(data: DataFrame, columns: Option[Array[String]] = None): Boolean = {
    val kData = apply(data, columns)
    val minCount = kData
      .agg(F.min("count").as("min"))
      .first()
      .getAs[Long]("min")
    minCount >= k
  }

  /**
   * Determines if a dataframe satisfies the conditions of K-Anonymity.
   *
   * @param data          The dataframe to be checked for K-Anonymity.
   * @return `true` if the dataframe satisfies K-Anonymity,
   *         `false` if not,
   */
  def apply(data: DataFrame, columns: Option[Array[String]] = None): DataFrame = {
    getHashedData(data, columns)
      .groupBy(F.col("row_hash"))
      .agg(F.count("*").as("count"))
  }
}
