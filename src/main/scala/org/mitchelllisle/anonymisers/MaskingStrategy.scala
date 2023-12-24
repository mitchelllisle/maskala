package org.mitchelllisle.anonymisers

import org.apache.spark.sql.{DataFrame, functions => F}

case class MaskingParams(mask: String) extends AnonymisationParams

case class MaskingStrategy(column: String) extends AnonymiserStrategy {
  override def apply(data: DataFrame, params: AnonymisationParams): DataFrame = {
    params match {
      case mp: MaskingParams => data.withColumn(column, F.lit(mp.mask))
      case _                 => throw new IllegalArgumentException("Invalid configuration for MaskingStrategy")
    }
  }
}
