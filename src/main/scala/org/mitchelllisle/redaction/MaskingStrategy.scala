package org.mitchelllisle.redaction

import org.apache.spark.sql.{DataFrame, functions => F}

case class MaskingStrategy(column: String, mask: String = "*") extends RedactionStrategy {
  override def apply(data: DataFrame): DataFrame = {
    data.withColumn(column, F.lit(mask))
  }
}
