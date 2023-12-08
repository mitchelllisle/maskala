import org.mitchelllisle.redaction.MaskingStrategy

class MaskingStrategyTest extends SparkFunSuite {
  "MaskingStrategy with defaults" should "alter column" in {
    val strategy = MaskingStrategy("user_id")

    val redactedData = strategy(sampleNetflixData)
    val results = redactedData.collect().map(_.getString(0))

    assert(results.forall(_.equals("*")))
  }
  "MaskingStrategy with custom mask" should "alter column" in {
    val mask = "{REDACTED}"
    val strategy = MaskingStrategy("user_id", mask)

    val redactedData = strategy(sampleNetflixData)
    val results = redactedData.collect().map(_.getString(0))
    assert(results.forall(_.equals(mask)))
  }
}
