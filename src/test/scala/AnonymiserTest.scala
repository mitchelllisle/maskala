import org.mitchelllisle.Anonymiser
import org.scalatest.flatspec.AnyFlatSpec

class AnonymiserTest extends AnyFlatSpec with SparkFunSuite {
  val configPath = "src/test/resources/sampleConfig.yaml"

  "Reading anonymiser config" should "return the right data structure" in {
    val anonymiser = new Anonymiser(configPath)
    val config = anonymiser.readConfig(configPath)
    assert(config.catalog == "bronze")
    assert(config.schema == "netflix")
    assert(config.table == "ratings")
    assert(config.anonymise.length == 6)
    assert(config.analyse.length == 3)
  }

  //  TODO finish test
  "Running anonymiser" should "use the provided strategies" in {
    val anonymiser = new Anonymiser(configPath)
    val anonymised = anonymiser.runAnonymisers(sampleNetflixData)
    anonymised.show()
  }
}
