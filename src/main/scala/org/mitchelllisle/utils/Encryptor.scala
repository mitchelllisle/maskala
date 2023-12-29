package org.mitchelllisle.utils

import org.apache.spark.sql.DataFrame
import org.mitchelllisle.anonymisers.SparkEncryptionUtil

import java.util.Base64
import javax.crypto.spec.{IvParameterSpec, SecretKeySpec}
import javax.crypto.{Cipher, KeyGenerator, SecretKey}

/** A utility class for encrypting and decrypting strings using the AES algorithm in CBC mode with PKCS5Padding. It
  * leverages a provided SecretKey for cryptographic operations.
  *
  * @constructor
  *   Create a new Encryptor instance with a provided SecretKey.
  * @param secret
  *   The SecretKey used for encryption and decryption operations.
  */
class Encryptor(secret: SecretKey) {

  @transient private lazy val cipher: Cipher = Cipher.getInstance("AES/CBC/PKCS5Padding")
  private val blockSize = cipher.getBlockSize

  /** Encrypts a string value using AES/CBC/PKCS5Padding.
    *
    * Initializes the cipher in encrypt mode with an IV (initialization vector) and applies AES encryption to the given
    * string. The resulting encrypted byte array is encoded into a Base64 string.
    *
    * @param value
    *   The string to encrypt.
    * @return
    *   The encrypted string, encoded in Base64.
    */
  def encrypt(value: String): String = {
    val ivSpec = new IvParameterSpec(new Array[Byte](blockSize))
    cipher.init(Cipher.ENCRYPT_MODE, secret, ivSpec)

    val encrypted = cipher.doFinal(value.getBytes("UTF-8"))
    Base64.getEncoder.encodeToString(encrypted)
  }

  /** Decrypts a string encrypted with AES/CBC/PKCS5Padding.
    *
    * Initializes the cipher in decrypt mode with an IV (initialization vector) and applies AES decryption to the given
    * Base64 encoded string. The decrypted byte array is converted back to a UTF-8 string.
    *
    * @param encryptedValue
    *   The encrypted string in Base64 format to decrypt.
    * @return
    *   The decrypted string.
    */
  def decrypt(encryptedValue: String): String = {
    val ivSpec = new IvParameterSpec(new Array[Byte](blockSize))
    cipher.init(Cipher.DECRYPT_MODE, secret, ivSpec)

    val decodedBytes = Base64.getDecoder.decode(encryptedValue)
    val decryptedBytes = cipher.doFinal(decodedBytes)
    new String(decryptedBytes, "UTF-8")
  }

  /** Encrypts specified columns in a DataFrame using the `SparkEncryptionUtil.encryptUDF` utility.
    *
    * Iterates through the specified columns and applies the AES encryption to each column's data.
    *
    * @param df
    *   The DataFrame to be processed.
    * @param columns
    *   A sequence of column names to encrypt.
    * @return
    *   A DataFrame with specified columns encrypted.
    */
  def encrypt(df: DataFrame, columns: Seq[String]): DataFrame = {
    columns.foldLeft(df) { (dataFrame, column) =>
      val func = SparkEncryptionUtil.encryptUDF(secret)
      dataFrame.withColumn(column, func(dataFrame(column)))
    }
  }

  /** Decrypts specified columns in a DataFrame using the `SparkEncryptionUtil.decryptUDF` utility.
    *
    * Iterates through the specified columns and applies the AES decryption to each column's data.
    *
    * @param df
    *   The DataFrame to be processed.
    * @param columns
    *   A sequence of column names to decrypt.
    * @return
    *   A DataFrame with specified columns decrypted.
    */
  def decrypt(df: DataFrame, columns: Seq[String]): DataFrame = {
    columns.foldLeft(df) { (dataFrame, column) =>
      val func = SparkEncryptionUtil.decryptUDF(secret)
      dataFrame.withColumn(column, func(dataFrame(column)))
    }
  }
}

/** Companion object for the Encryptor class. Provides a utility method to generate a SecretKey for AES encryption.
  */
object Encryptor {

  /** Generates a SecretKey for AES encryption.
    *
    * Creates a KeyGenerator instance for the AES algorithm and initializes it with the specified key size.
    *
    * @param keySize
    *   The size of the key in bits. Default is 256.
    * @return
    *   A newly generated SecretKey for AES encryption.
    */
  def generateSecret(keySize: Int = 256): SecretKey = {
    val keyGen = KeyGenerator.getInstance("AES")
    keyGen.init(keySize)
    val secretKey: SecretKey = keyGen.generateKey()
    secretKey
  }

  /** Converts a SecretKey to its string representation.
    *
    * Encodes the byte representation of the SecretKey using Base64 encoding.
    *
    * @param secret
    *   The SecretKey to be converted to a string.
    * @return
    *   A Base64 encoded string representation of the SecretKey.
    */
  def keyToString(secret: SecretKey): String = {
    Base64.getEncoder.encodeToString(secret.getEncoded)
  }

  /** Converts a string representation of a SecretKey back to its SecretKey form.
    *
    * Decodes the Base64 encoded string into bytes and creates a new SecretKeySpec for AES encryption.
    *
    * @param secret
    *   The Base64 encoded string representation of the SecretKey.
    * @return
    *   The corresponding SecretKey.
    */
  def stringToKey(secret: String): SecretKey = {
    val decoded = Base64.getDecoder.decode(secret)
    new SecretKeySpec(decoded, "AES")
  }
}
