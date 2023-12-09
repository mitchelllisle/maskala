package org.mitchelllisle.redaction

import org.apache.spark.sql.{DataFrame, Column, functions => F}

/**
 * HashingStrategy class is used for redacting sensitive data in a DataFrame by applying a hashing function.
 * The design paradigm adopted allows hashing with additional security measures such as 'salt' and 'pepper'
 * to further decrease the likelihood of hash collision and make it more resistant to rainbow table attacks.
 *
 * @param column The dataframe column intended for redaction via hashing.
 */
case class HashingStrategy(column: String) extends RedactionStrategy {

  /**
   * Computes the hash of a given column value using the SHA-256 algorithm.
   * SHA-256 was chosen as it provides a good balance between security and performance.
   *
   * @param column The DataFrame column whose values should be hashed.
   * @return A Column with hashed values.
   */
  private def hashColumn(column: Column): Column = F.sha2(column, 256)

  /**
   * Combines a salt value, a column value, and a pepper value in order.
   * The choice was made to allow optional salt and pepper for versatility of use case scenarios.
   * If salt and pepper are not provided, the column value remains unaltered.
   *
   * @param value  DataFrame column to which the salt and pepper will be added.
   * @param salt   Optional salt value to prepend to the column value.
   * @param pepper Optional pepper value to append to the column value.
   * @return A Column where each row's value is the concatenation of supplied salt, original value, and pepper.
   */
  private def concatSaltAndPepper(value: Column, salt: String, pepper: String): Column = {
    F.concat(F.lit(salt), value, F.lit(pepper))
  }

  /**
   * Applies the hashColumn function to a DataFrame, operating over a specified column.
   * If no salt and pepper are provided, this hashes the original values.
   *
   * @param data The DataFrame to be processed.
   * @return A new DataFrame with values in the specified column replaced by their securely hashed versions.
   */
  def apply(data: DataFrame): DataFrame = {
    data.withColumn(column, hashColumn(F.col(column)))
  }

  /**
   * Applies the hashColumn function to a DataFrame over a specified column, preceded by optional salt and followed by optional pepper.
   *
   * @param data The DataFrame to be processed.
   * @param salt The optional 'salt' value to add before each original value prior to hashing for increased security.
   * @param pepper The optional 'pepper' value to append to each original value prior to hashing for increased security.
   * @return A new DataFrame with hashes of concatenated salt, original value, and pepper in their respective columns.
   */
  def apply(data: DataFrame, salt: String = "", pepper: String = ""): DataFrame = {
    data.withColumn(column, hashColumn(concatSaltAndPepper(F.col(column), salt, pepper)))
  }
}
