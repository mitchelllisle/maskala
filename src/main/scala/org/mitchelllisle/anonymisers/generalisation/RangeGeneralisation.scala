package org.mitchelllisle.anonymisers.generalisation

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

/** A class representing a generalisation strategy that applies range generalisation to a DataFrame.
  *
  * @param columnName
  *   The name of the column on which the generalisation is applied.
  * @param rangeWidth
  *   The width of each range for the generalisation.
  * @param separator
  *   The separator used to concatenate the lower and upper bounds in the generalised values.
  */
case class RangeGeneralisation(columnName: String, rangeWidth: Int, separator: String = "-")
    extends GeneralisationStrategy {
  override def apply(data: DataFrame): DataFrame = {
    val lowerBound = floor(col(columnName) / rangeWidth) * rangeWidth
    val upperBound = lowerBound + rangeWidth - 1

    data.withColumn(columnName, concat(lowerBound.cast("string"), lit(separator), upperBound.cast("string")))
  }
}
