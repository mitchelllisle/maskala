package org.mitchelllisle.anonymisers

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{DataFrame, functions => F}
import javax.crypto.SecretKey
import org.mitchelllisle.utils.Encryptor

/** Represents parameters for encryption-based anonymisation.
 *
 * @param secret A string used as a basis for generating a SecretKey for encryption and decryption.
 *               This should be a strong, unique identifier that can be securely stored and accessed.
 */
case class EncryptionParams(secret: String) extends AnonymisationParams

/** Utility object providing UDFs for encryption and decryption in Spark DataFrames.
 *
 * This object includes methods to create UDFs for encrypting and decrypting string data.
 * The UDFs use the provided SecretKey to perform the cryptographic operations.
 */
object SparkEncryptionUtil {

  /** Creates a UDF for encrypting strings in a DataFrame. We need this util so that there is a serialisable object
   * we can pass to a Spark operation. Without this we were running into can't serialise errors.
   *
   * @param secretKey The SecretKey used for encryption.
   * @return A UserDefinedFunction that takes a string and returns its encrypted form.
   */
  def encryptUDF(secretKey: SecretKey): UserDefinedFunction = F.udf((plaintext: String) => {
    val encryptor = new Encryptor(secretKey)
    if (plaintext != null) encryptor.encrypt(plaintext) else null
  })

  /** Creates a UDF for decrypting strings in a DataFrame.
   *
   * @param secretKey The SecretKey used for decryption.
   * @return A UserDefinedFunction that takes an encrypted string and returns its plaintext form.
   */
  def decryptUDF(secretKey: SecretKey): UserDefinedFunction = F.udf((cipherText: String) => {
    val encryptor = new Encryptor(secretKey)
    if (cipherText != null) encryptor.decrypt(cipherText) else null
  })
}

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
        val crypt = SparkEncryptionUtil.encryptUDF(secretKey)
        data.withColumn(column, crypt(data(column)))
      case _ => throw new IllegalArgumentException("Invalid configuration for EncryptionStrategy")
    }
  }
}
