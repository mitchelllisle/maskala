package org.mitchelllisle.khyperloglog

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{Column, DataFrame, SparkSession, functions => F}

class UniquenessAnalyser(spark: SparkSession) {

  val fieldCol: Column = F.col("field")
  val idCol: Column = F.col("id")

  private val uniquenessCol: Column = F.col("uniqueness")

  def getTable(schema: String, table: String, primaryKey: String, columns: Seq[String]): DataFrame = {
    val value = F.concat(columns.map(F.col): _*).cast(StringType)
    val id = F.col(primaryKey).cast(StringType)

    spark.read
      .table(s"$schema.$table")
      .select(value, id)
      .toDF("field", "id")
      .na.drop() // we can't hash a null value
  }

  def uniquenessData(data: DataFrame): DataFrame = {
    data.groupBy(fieldCol).agg(F.countDistinct(idCol).as("uniqueness"))
  }

  def numValues(data: DataFrame): Long = {
    data.count()
  }

  def uniquenessDistribution(data: DataFrame, totalCount: Long): DataFrame = {
    data
      .groupBy(uniquenessCol)
      .agg(
        F.count(fieldCol).as("valueCount"),
        (F.count(fieldCol) / totalCount).as("valueRatio")
      )
      .orderBy(uniquenessCol)
  }

  def run(data: DataFrame): DataFrame = {
    val unique = uniquenessData(data)
    val totalValues = numValues(data)

    val distribution = uniquenessDistribution(unique, totalValues)

    val windowSpec = Window.orderBy("uniqueness").rowsBetween(Window.unboundedPreceding, Window.currentRow)

    distribution
      .withColumn("cumulative_value_count", F.sum("valueCount").over(windowSpec))
      .withColumn("cumulative_value_ratio", F.sum("valueRatio").over(windowSpec))
  }
}
