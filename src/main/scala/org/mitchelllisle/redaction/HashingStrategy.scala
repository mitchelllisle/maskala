package org.mitchelllisle.redaction

import org.apache.spark.sql.{DataFrame, functions => F}

case class HashingStrategy(column: String) extends RedactionStrategy {
  override def apply(data: DataFrame): DataFrame = {
    data.withColumn(column, F.sha2(data(column), 256))
  }
}
