package org.mitchelllisle.khyperloglog

import org.apache.spark.sql.{DataFrame, SparkSession, functions => F}
import org.apache.datasketches.hll.HllSketch
import org.apache.spark.rdd.RDD

case class KHLL(id: String, hllSketch: HllSketch)
case class KLLRow(id: String, field: String)

class KHyperLogLogAnalyser(spark: SparkSession, k: Int) {

  private val fieldCol = F.col("field")
  private val idCol = F.col("id")


  def getTable(schema: String, table: String, primaryKey: String, columns: Seq[String]): DataFrame = {
    val value = F.concat(columns.map(F.col): _*)
    val id = F.col(primaryKey)

    spark.read
      .table(s"$schema.$table")
      .select(value, id)
      .toDF("field", "id")
      .na.drop()
  }

  def hashFieldCol(data: DataFrame): DataFrame = {
    data
      .select(fieldCol)
      .withColumn("field", F.hash(fieldCol))
      .dropDuplicates()
      .orderBy(fieldCol)
      .limit(k)
  }

  def hashIDCol(data: DataFrame): DataFrame = {
    data.withColumn("field", F.hash(fieldCol))
  }

  def khll(fieldHashes: DataFrame, idHashes: DataFrame): RDD[KHLL] = {
    val joined = fieldHashes.join(fieldHashes, fieldHashes("field") === idHashes("field"))
    joined.rdd.map(row => {
      val id = row.getString(1)
      val hll = new HllSketch(10)
      hll.update(id)
      KHLL(id, hll)
    })
  }
}
