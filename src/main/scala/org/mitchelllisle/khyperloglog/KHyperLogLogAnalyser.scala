package org.mitchelllisle.khyperloglog

import org.apache.spark.sql.{DataFrame, SparkSession, functions => F}
import com.swoop.alchemy.spark.expressions.hll.functions.hll_init

case class KLLRow(id: String, field: String)

class KHyperLogLogAnalyser(spark: SparkSession, k: Int) extends UniquenessAnalyser(spark) {

  def hashFieldCol(data: DataFrame): DataFrame = {
    data
      .select(fieldCol)
      .withColumn("field", F.hash(fieldCol))
      .orderBy(fieldCol)
      .limit(k)
  }

  def hashIDCol(data: DataFrame): DataFrame = {
    data.withColumn("field", F.hash(fieldCol))
  }

  def khll(fieldHashes: DataFrame, idHashes: DataFrame): DataFrame = {
    fieldHashes
      .join(idHashes, fieldHashes("field") === idHashes("field"))
      .withColumn("hll", hll_init(idCol))
  }

}
