package org.mitchelllisle.reidentifiability

import com.google.common.annotations.Beta
import org.apache.spark.sql.{DataFrame, SparkSession, functions => F}
import com.swoop.alchemy.spark.expressions.hll.functions.hll_init

@Beta
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
