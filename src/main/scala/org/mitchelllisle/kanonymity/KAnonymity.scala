package org.mitchelllisle.kanonymity

import org.apache.spark.sql.{DataFrame, functions => F}
import org.mitchelllisle.generalisation.GeneralisationStrategy

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
   * @param df      The dataframe to which the hash column will be added.
   * @param columns The columns which will be used to generate the hash.
   * @return A new dataframe with an additional "row_hash" column.
   */
  private def getHashedData(df: DataFrame, columns: Array[String]): DataFrame =
    df.withColumn("row_hash", hashUdf(F.concat_ws("|", columns.map(df(_)): _*)))

  /**
   * Computes frequency counts of unique rows in the dataframe while considering specific columns.
   *
   * @param data          The dataframe whose rows' frequencies will be computed.
   * @param ignoreColumns Columns to ignore while computing the row frequencies.
   * @return A dataframe with a "row_hash" and a "count" column.
   */
  private def getRowFrequencyCounts(data: DataFrame, ignoreColumns: Seq[String] = Seq.empty): DataFrame = {
    val columnsToConsider = data.columns.filterNot(ignoreColumns.contains)
    getHashedData(data, columnsToConsider)
      .groupBy(F.col("row_hash"))
      .agg(F.count("*").as("count"))
  }

  /**
   * Filters the dataframe to only include rows whose frequencies meet a minimum threshold (k).
   *
   * @param data          The dataframe to be filtered.
   * @param ignoreColumns Columns to ignore while determining row uniqueness.
   * @return A dataframe with rows that meet the minimum frequency threshold.
   */
  def filter(data: DataFrame, ignoreColumns: Seq[String] = Seq.empty): DataFrame = {
    val columnsToConsider = data.columns.filterNot(ignoreColumns.contains)
    val countsDf = getRowFrequencyCounts(data, ignoreColumns)
    val hashedData = getHashedData(data, columnsToConsider)

    hashedData
      .join(countsDf, "row_hash")
      .filter(F.col("count") >= k)
      .drop("count", "row_hash")
  }


  /**
   * Determines if a dataframe satisfies the conditions of K-Anonymity.
   *
   * @param data          The dataframe to be checked for K-Anonymity.
   * @param ignoreColumns Columns to ignore while determining row uniqueness.
   * @return `true` if the dataframe satisfies K-Anonymity,
   *         `false` if not,
   */
  def apply(data: DataFrame, ignoreColumns: Seq[String] = Seq.empty): Boolean = {
    val countsDf = getRowFrequencyCounts(data, ignoreColumns)
    val minCount = countsDf
      .agg(F.min("count").as("min"))
      .first()
      .getAs[Long]("min")
    minCount >= k
  }
}
