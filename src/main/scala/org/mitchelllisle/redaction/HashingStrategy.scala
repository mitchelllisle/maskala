package org.mitchelllisle.redaction

import org.apache.spark.sql.{DataFrame, functions => F}

/**
 * The HashingStrategy class is a redaction strategy that applies a hashing function to the values in a specified column of a DataFrame.
 * This strategy is useful for transforming sensitive data into a hashed format, thereby enhancing data privacy and security.
 *
 * @param column The name of the column in the DataFrame whose values will be hashed.
 */
case class HashingStrategy(column: String) extends RedactionStrategy {
  /**
   * Applies the hashing strategy to the specified DataFrame.
   * This method uses the SHA-256 hashing algorithm to hash all values in the specified column.
   *
   * @param data The DataFrame on which the hashing will be applied.
   * @return A DataFrame with the values in the specified column replaced by their hashed equivalents.
   */
  override def apply(data: DataFrame): DataFrame = {
    data.withColumn(column, F.sha2(data(column), 256))
  }
}
