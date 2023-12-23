import org.mitchelllisle.anonymisers.redaction.{HashingStrategy, MaskingStrategy, Redactor}
import org.scalatest.flatspec.AnyFlatSpec

class RedactorTest extends AnyFlatSpec with SparkFunSuite {
  "Giving multiple strategies" should "apply the right transformations" in {
    val mask = "{REDACTED}"
    val strategies = Seq(
      HashingStrategy("user_id"),
      MaskingStrategy("movie", mask)
    )
    val redactor = new Redactor(strategies)
    val redacted = redactor(sampleNetflixData)

    redacted.collect().foreach(row => {
      val hashed = row.getString(0)
      val masked = row.getString(3)

      assert(hashed.length == 64)
      assert(masked == mask)
    })
  }
}
