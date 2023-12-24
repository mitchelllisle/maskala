package org.mitchelllisle.anonymisers

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{DataFrame, functions => F}

import javax.crypto.SecretKey
import org.mitchelllisle.utils.Encryptor

case class EncryptionParams(secret: String) extends AnonymisationParams

object SparkEncryptionUtil {
  def encryptUDF(secretKey: SecretKey): UserDefinedFunction = F.udf((plaintext: String) => {
    val encryptor = new Encryptor(secretKey)
    if (plaintext != null) encryptor.encrypt(plaintext) else null
  })

  def decryptUDF(secretKey: SecretKey): UserDefinedFunction = F.udf((cipherText: String) => {
    val encryptor = new Encryptor((secretKey))
    if (cipherText != null) encryptor.decrypt(cipherText) else null
  })
}

case class EncryptionStrategy(column: String) extends AnonymiserStrategy {
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
