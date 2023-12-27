package org.mitchelllisle.anonymisers

import org.apache.spark.sql.{DataFrame, functions => F}

/** Represents parameters for range-based anonymisation.
 *
 * @param rangeWidth An integer value defining the width of the range.
 *                   It determines how values are grouped into ranges.
 * @param separator A string used to separate the lower and upper bounds of the range in the final output.
 */
case class RangeParams(rangeWidth: Int = 10, separator: String = "-") extends AnonymisationParams

/** Implements a strategy for anonymising numeric data by applying range generalisation in a DataFrame.
 *
 * This strategy takes a numeric column and replaces each value with a range it falls into.
 * The range is defined based on `rangeWidth` from `RangeParams`.
 * Each value is mapped to a range, represented as 'lowerBound-upperBound'.
 *
 * @param column The name of the column in the DataFrame on which the range generalisation is applied.
 */
case class RangeStrategy(column: String) extends AnonymiserStrategy {

  /** Applies the range generalisation strategy to a DataFrame.
   *
   * This method computes the lower and upper bounds of the range for each value in the specified column.
   * It then replaces the original value with a string representing the range.
   * The format of the range string is 'lowerBound-upperBound', with the bounds separated by `separator` from `RangeParams`.
   *
   * @param data The DataFrame to be anonymised.
   * @param config The configuration parameters for the range, encapsulated in a `RangeParams` instance.
   * @return A DataFrame with the range generalisation applied to the specified column.
   * @throws IllegalArgumentException if `config` is not an instance of `RangeParams`.
   */
  override def apply(data: DataFrame, config: AnonymisationParams): DataFrame = {
    config match {
      case rp: RangeParams => {
        val lowerBound = F.floor(F.col(column) / rp.rangeWidth) * rp.rangeWidth
        val upperBound = lowerBound + rp.rangeWidth - 1

        data.withColumn(column, F.concat(lowerBound.cast("string"), F.lit(rp.separator), upperBound.cast("string")))
      }
      case _ => throw new IllegalArgumentException("Invalid configuration for RangeStrategy")
    }
  }
}
