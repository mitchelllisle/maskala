import org.mitchelllisle.reidentifiability.KHyperLogLogAnalyser

class KHLLAnalyserTest extends SparkFunSuite {
  val k = 2056
  val khll = new KHyperLogLogAnalyser(spark)

  "createSourceTable" should "prepare the data with hash values" in {
    val prepared = khll.createSourceTable(sampleNetflixData, Seq("date", "rating"), "customerId")

    assert(prepared.columns.sameElements(Array("value", "id")))
    assert(prepared.count() == 9999)
  }

  "createKHHLTable" should "prepare the data with hash values" in {
    val prepared = khll.createSourceTable(sampleNetflixData, Seq("date", "rating"), "customerId")
    val table = khll.createKHHLTable(prepared)

    assert(prepared.columns.sameElements(Array("value", "id")))
    assert(prepared.count() == 9999)
  }

}
