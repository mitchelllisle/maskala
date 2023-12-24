package org.mitchelllisle.anonymisers

import org.apache.spark.sql.{DataFrame, functions => F}

case class RangeParams(rangeWidth: Int = 10, separator: String = "-") extends AnonymisationParams

/** A class representing a generalisation strategy that applies range generalisation to a DataFrame.
  *
  * @param column
  *   The name of the column on which the generalisation is applied.
  */
case class RangeStrategy(column: String) extends AnonymiserStrategy {
  def apply(data: DataFrame, config: AnonymisationParams): DataFrame = {
    config match {
      case rp: RangeParams => {
        val lowerBound = F.floor(F.col(column) / rp.rangeWidth) * rp.rangeWidth
        val upperBound = lowerBound + rp.rangeWidth - 1

        data.withColumn(column, F.concat(lowerBound.cast("string"), F.lit(rp.separator), upperBound.cast("string")))
      }
    }
  }
}
