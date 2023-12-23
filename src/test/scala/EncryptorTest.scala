import org.scalatest.flatspec.AnyFlatSpec
import org.mitchelllisle.encryption.Encryptor

class EncryptorTest extends AnyFlatSpec {
  private val secret = Encryptor.generateSecret()

  "Encryption" should "alter the plaintext value" in {
    val plainText = "Hello World"
    val crypt = new Encryptor(secret)
    val cipherText = crypt.encrypt(plainText)
    assert(cipherText != plainText)
  }

  "Decryption" should "recover the original plaintext" in {
    val plainText = "Hello World"
    val crypt = new Encryptor(secret)
    val cipherText = crypt.encrypt(plainText)
    val decryptedPlainText = crypt.decrypt(cipherText)
    assert(decryptedPlainText == plainText)
  }
}
