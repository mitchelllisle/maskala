package org.mitchelllisle.redaction

import org.apache.spark.sql.{DataFrame, functions => F}

/**
 * The MaskingStrategy class is a redaction strategy that replaces the values in a specified column of a DataFrame with a mask.
 * This class is useful for obfuscating sensitive data in a DataFrame column, such as personal identifiers or confidential information.
 *
 * @param column The name of the column in the DataFrame whose values will be replaced by the mask.
 * @param mask The string that will replace each value in the specified column. Default is a single asterisk "*".
 */
case class MaskingStrategy(column: String, mask: String = "*") extends RedactionStrategy {
  /**
   * Applies the masking strategy to the specified DataFrame.
   * This method replaces all values in the specified column with the provided mask.
   *
   * @param data The DataFrame on which the redaction will be applied.
   * @return A DataFrame with the specified column's values replaced by the mask.
   */
  override def apply(data: DataFrame): DataFrame = {
    data.withColumn(column, F.lit(mask))
  }
}
