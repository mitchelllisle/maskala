import org.mitchelllisle.utils.Encryptor
import org.scalatest.flatspec.AnyFlatSpec

class EncryptorTest extends AnyFlatSpec with SparkFunSuite {
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

  "String secret" should "be parsed into SecretKey" in {
    val secretVal = Encryptor.stringToKey("O2Ls0Y1EI9+HJAu0SdHsWD2ag/4RfwrqJUDcTpDYlZc=")
    val encryptor = new Encryptor(secretVal)
    encryptor.encrypt("Hello")
  }

  "SecretKey secret" should "be parsed into String and back again" in {
    val s = Encryptor.keyToString(secret)
    val newSecret = Encryptor.stringToKey(s)
    val encryptor = new Encryptor(newSecret)
    encryptor.encrypt("Message")
  }
}
