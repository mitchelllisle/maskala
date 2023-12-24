//package org.mitchelllisle.anonymisers
//
//import org.apache.spark.sql.expressions.UserDefinedFunction
//import org.apache.spark.sql.{DataFrame, functions => F}
//
//import java.util.Base64
//import javax.crypto.spec.IvParameterSpec
//import javax.crypto.{Cipher, KeyGenerator, SecretKey}
//
//object SparkEncryptionUtil {
//  def encryptUDF(secretKey: SecretKey): UserDefinedFunction = F.udf((plaintext: String) => {
//    val encryptor = new EncryptionStrategy((secretKey))
//    if (plaintext != null) encryptor.encrypt(plaintext) else null
//  })
//
//  def decryptUDF(secretKey: SecretKey): UserDefinedFunction = F.udf((cipherText: String) => {
//    val encryptor = new EncryptionStrategy((secretKey))
//    if (cipherText != null) encryptor.decrypt(cipherText) else null
//  })
//}
//
///** A class to handle encryption and decryption of strings using AES/CBC/PKCS5Padding. Requires a SecretKey for the AES
//  * algorithm.
//  *
//  * @constructor
//  *   Create a new Encryptor with a SecretKey.
//  * @param secret
//  *   The SecretKey used for encryption and decryption operations.
//  */
//class EncryptionStrategy(secret: SecretKey) {
//
//  @transient private lazy val cipher: Cipher = Cipher.getInstance(
//    "AES/CBC/PKCS5Padding")
//  private val blockSize = cipher.getBlockSize
//
//  def encrypt(value: String): String = {
//    val ivSpec = new IvParameterSpec(new Array[Byte](blockSize))
//    cipher.init(Cipher.ENCRYPT_MODE, secret, ivSpec)
//
//    val encrypted = cipher.doFinal(value.getBytes("UTF-8"))
//    Base64.getEncoder.encodeToString(encrypted)
//  }
//
//  def decrypt(encryptedValue: String): String = {
//    val ivSpec = new IvParameterSpec(new Array[Byte](blockSize))
//    cipher.init(Cipher.DECRYPT_MODE, secret, ivSpec)
//
//    val decodedBytes = Base64.getDecoder.decode(encryptedValue)
//    val decryptedBytes = cipher.doFinal(decodedBytes)
//    new String(decryptedBytes, "UTF-8")
//  }
//
//  /** Encrypts specified columns in a DataFrame.
//    *
//    * @param df
//    *   The DataFrame to be processed.
//    * @param columns
//    *   A sequence of column names to encrypt.
//    * @return
//    *   A DataFrame with specified columns encrypted.
//    */
//  def encrypt(df: DataFrame, columns: Seq[String]): DataFrame = {
//    columns.foldLeft(df) { (dataFrame, column) =>
//      val func = SparkEncryptionUtil.encryptUDF(secret)
//      dataFrame.withColumn(column, func(dataFrame(column)))
//    }
//  }
//
//  def decrypt(df: DataFrame, columns: Seq[String]): DataFrame = {
//    columns.foldLeft(df) { (dataFrame, column) =>
//      val func = SparkEncryptionUtil.decryptUDF(secret)
//      dataFrame.withColumn(column, func(dataFrame(column)))
//    }
//  }
//}
//
///** Companion object for the Encryptor class. Provides a utility method to generate a SecretKey for AES encryption.
//  */
//object EncryptionStrategy {
//
//  /** Generates a SecretKey for AES encryption.
//    *
//    * @param keySize
//    *   The size of the key in bits. Default is 256.
//    * @return
//    *   A SecretKey for AES encryption.
//    */
//  def generateSecret(keySize: Int = 256): SecretKey = {
//    val keyGen = KeyGenerator.getInstance("AES")
//    keyGen.init(keySize)
//    val secretKey: SecretKey = keyGen.generateKey()
//    secretKey
//  }
//}
