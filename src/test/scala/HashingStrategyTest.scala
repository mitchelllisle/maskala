import org.mitchelllisle.redaction.HashingStrategy


class HashingStrategyTest extends SparkFunSuite {

  "HashingStrategy" should "correctly hash values based on column name" in {
    val strategy = HashingStrategy("user_id")

    val redactedData = strategy(sampleNetflixData)
    val results = redactedData.collect().map(_.getString(0))

    val rawUserIds = sampleNetflixData.collect().map(_.getString(0))

    assert(results.forall(_.length == 64))
    assert(!results.sameElements(rawUserIds))
  }
}
