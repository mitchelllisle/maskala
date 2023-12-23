import org.mitchelllisle.Anonymiser
import org.scalatest.flatspec.AnyFlatSpec

class AnonymiserTest extends AnyFlatSpec {
  val configPath = "src/test/resources/sampleConfig.yaml"

  "Reading anonymiser config" should "return the right data structure" in {
    val anonymiser = new Anonymiser(configPath)
    val config = anonymiser.readConfig()
    assert(config.catalog == "bronze")
    assert(config.schema == "netflix")
    assert(config.table == "ratings")
    assert(config.anonymise.length == 4)
    assert(config.analyse.length == 3)
  }
}
