package org.mitchelllisle.anonymisers

import org.apache.spark.sql.DataFrame
import org.mitchelllisle.utils.Encryptor

/** Represents parameters for encryption-based anonymisation.
 *
 * @param secret A string used as a basis for generating a SecretKey for encryption and decryption.
 *               This should be a strong, unique identifier that can be securely stored and accessed.
 */
case class EncryptionParams(secret: String) extends AnonymisationParams

/** Implements a strategy for anonymising data by encrypting the values in a specified column.
 *
 * This strategy uses cryptographic encryption to anonymise data. It takes a column name and
 * applies encryption to each of its values based on a provided SecretKey.
 * The SecretKey is derived from the `EncryptionParams` provided.
 *
 * @param column The name of the column in the DataFrame whose values are to be encrypted.
 */
case class EncryptionStrategy(column: String) extends AnonymiserStrategy {

  /** Applies the encryption strategy to a DataFrame.
   *
   * This method uses the `encryptUDF` function from `SparkEncryptionUtil` to encrypt values in the specified column.
   * The SecretKey for encryption is derived from the provided `EncryptionParams`.
   * If the `params` argument is not an instance of `EncryptionParams`, the method will throw an IllegalArgumentException.
   *
   * @param data The DataFrame to be anonymised.
   * @param params The parameters for encryption, encapsulated in an `EncryptionParams` instance.
   * @return A DataFrame with the encryption applied to the specified column.
   * @throws IllegalArgumentException if `params` is not an instance of `EncryptionParams`.
   */
  override def apply(data: DataFrame, params: AnonymisationParams): DataFrame = {
    params match {
      case ep: EncryptionParams =>
        val secretKey = Encryptor.stringToKey(ep.secret)
        val encryptor = new Encryptor(secretKey)
        encryptor.encrypt(data, Seq(column))
      case _ => throw new IllegalArgumentException("Invalid configuration for EncryptionStrategy")
    }
  }
}
