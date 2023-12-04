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
    val table = khll.createKHLLTable(prepared)

    assert(prepared.columns.sameElements(Array("value", "id")))
    assert(prepared.count() == 9999)
  }

  "generating end to end" should "return the right cardinality" in {
    val table = khll(sampleNetflixData, Seq("date", "rating"), "customerId", k)

    assert(
      table
        .columns
        .sameElements(
          Array("hll", "estimatedValueCount", "estimatedValueRatio", "cumulativeValueCount", "cumulativeValueRatio")
        )
    )
    assert(table.count() == 21)
  }
}
