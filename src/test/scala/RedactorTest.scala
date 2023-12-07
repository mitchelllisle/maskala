import org.mitchelllisle.redaction.{HashingStrategy, Redactor}

class RedactorTest extends SparkFunSuite {

  "Giving multiple strategies" should "apply the right transformations" in {
    val strategies = Seq(
      HashingStrategy("user_id")
    )
    val redactor = new Redactor(strategies)
    val redacted = redactor(sampleNetflixData)

    assert(redacted.collect().forall(_.getString(0).length == 64))
  }
}
