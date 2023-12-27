package org.mitchelllisle.anonymisers

import org.apache.spark.sql.{DataFrame, functions => F}

/** Represents parameters for masking-based anonymisation.
 *
 * @param mask A string used to replace the values in the specified column.
 *             This mask will be applied uniformly to all values in the column,
 *             effectively hiding the original data.
 */
case class MaskingParams(mask: String) extends AnonymisationParams

/** Implements a strategy for anonymising data by masking the values in a specified column.
 *
 * This strategy takes a column name and replaces all its values with a predefined mask.
 * The mask is defined in `MaskingParams`. This approach is useful for hiding sensitive data
 * where the actual value is not critical for analysis, such as personally identifiable information.
 *
 * @param column The name of the column in the DataFrame whose values are to be masked.
 */
case class MaskingStrategy(column: String) extends AnonymiserStrategy {

  /** Applies the masking strategy to a DataFrame.
   *
   * This method replaces the values in the specified column with the mask provided in `MaskingParams`.
   * If the `params` argument is not an instance of `MaskingParams`, the method will throw an IllegalArgumentException.
   *
   * @param data The DataFrame to be anonymised.
   * @param params The parameters defining the mask. Must be an instance of `MaskingParams`.
   * @return A DataFrame with the masking applied to the specified column.
   * @throws IllegalArgumentException if `params` is not an instance of `MaskingParams`.
   */
  override def apply(data: DataFrame, params: AnonymisationParams): DataFrame = {
    params match {
      case mp: MaskingParams => data.withColumn(column, F.lit(mp.mask))
      case _ => throw new IllegalArgumentException("Invalid configuration for MaskingStrategy")
    }
  }
}
