package org.mitchelllisle.generalisation

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._


case class RangeGeneralisation(columnName: String, rangeWidth: Int, separator: String = "-") extends GeneralisationStrategy {
  override def apply(data: DataFrame): DataFrame = {
    val lowerBound = floor(col(columnName) / rangeWidth) * rangeWidth
    val upperBound = lowerBound + rangeWidth - 1

    data.withColumn(columnName, concat(lowerBound.cast("string"), lit(separator), upperBound.cast("string")))
  }
}
