package org.mitchelllisle.encryption

import javax.crypto.Cipher
import javax.crypto.SecretKey
import javax.crypto.KeyGenerator
import javax.crypto.spec.IvParameterSpec
import java.util.Base64

/** A class to handle encryption and decryption of strings using AES/CBC/PKCS5Padding. Requires a SecretKey for the AES
  * algorithm.
  *
  * @constructor
  *   Create a new Encryptor with a SecretKey.
  * @param secret
  *   The SecretKey used for encryption and decryption operations.
  */
class Encryptor(secret: SecretKey) {

  private val cipher = Cipher.getInstance("AES/CBC/PKCS5Padding")
  private val blockSize = new Array[Byte](cipher.getBlockSize)

  /** Encrypts a given string using AES/CBC/PKCS5Padding.
    *
    * @param value
    *   The string to be encrypted.
    * @return
    *   The encrypted string, encoded in Base64.
    */
  def encrypt(value: String): String = {
    val ivSpec = new IvParameterSpec(blockSize)
    cipher.init(Cipher.ENCRYPT_MODE, secret, ivSpec)

    val encrypted = cipher.doFinal(value.getBytes("UTF-8"))
    Base64.getEncoder.encodeToString(encrypted)
  }

  /** Decrypts a given encrypted string (in Base64 format) using AES/CBC/PKCS5Padding.
    *
    * @param encryptedValue
    *   The encrypted string in Base64 format to be decrypted.
    * @return
    *   The decrypted string.
    */
  def decrypt(encryptedValue: String): String = {
    val ivSpec = new IvParameterSpec(blockSize)
    cipher.init(Cipher.DECRYPT_MODE, secret, ivSpec)

    val decodedBytes = Base64.getDecoder.decode(encryptedValue)
    val decryptedBytes = cipher.doFinal(decodedBytes)
    new String(decryptedBytes, "UTF-8")
  }
}

/** Companion object for the Encryptor class. Provides a utility method to generate a SecretKey for AES encryption.
  */
object Encryptor {

  /** Generates a SecretKey for AES encryption.
    *
    * @param keySize
    *   The size of the key in bits. Default is 256.
    * @return
    *   A SecretKey for AES encryption.
    */
  def generateSecret(keySize: Int = 256): SecretKey = {
    val keyGen = KeyGenerator.getInstance("AES")
    keyGen.init(keySize)
    val secretKey: SecretKey = keyGen.generateKey()
    secretKey
  }
}
