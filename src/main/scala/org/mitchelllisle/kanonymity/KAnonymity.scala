package org.mitchelllisle.kanonymity

import org.apache.spark.sql.{DataFrame, functions => F}

import java.security.MessageDigest

class KAnonymity(k: Int) {
  private val hashUdf = F.udf[String, String](hashRow)

  private def hashRow(s: String): String =
    MessageDigest
      .getInstance("SHA-256")
      .digest(s.getBytes("UTF-8"))
      .map("%02x".format(_))
      .mkString

  private def getHashColumns(data: DataFrame, columns: Option[Array[String]]): Array[String] =
    columns.getOrElse(data.columns)

  private def getHashedData(data: DataFrame, columns: Option[Array[String]] = None): DataFrame = {
    val hashCols = getHashColumns(data, columns)
    data.withColumn("row_hash", hashUdf(F.concat_ws("|", hashCols.map(data(_)): _*)))
  }

  def removeLessThanKRows(data: DataFrame, columns: Option[Array[String]] = None): DataFrame = {
    val countedData = apply(data, columns)
    val columnsToHash = getHashColumns(data, columns)
    val hashedData = data.withColumn("row_hash", hashUdf(F.concat_ws("|", columnsToHash.map(data(_)): _*)))

    hashedData
      .join(countedData, "row_hash")
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

  def apply(data: DataFrame, columns: Option[Array[String]] = None): DataFrame = {
    val hashedData = getHashedData(data, columns)
    hashedData
      .groupBy(F.col("row_hash"))
      .agg(F.count("*").as("count"))
  }
}
