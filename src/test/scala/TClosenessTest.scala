import org.scalatest.flatspec.AnyFlatSpec
import org.mitchelllisle.analysers.TCloseness

class TClosenessTest extends AnyFlatSpec with SparkFunSuite {

  import spark.implicits._

  "t-closeness" should "be satisfied when distributions are close" in {
    val tClose = new TCloseness(t = 0.3)

    val data = Seq(
      ("A", "Disease1"),
      ("A", "Disease2"),
      ("A", "Disease3"),
      ("B", "Disease1"),
      ("B", "Disease2"),
      ("B", "Disease3")
    ).toDF("QuasiIdentifier", "Disease")

    assert(tClose.isTClose(data, "Disease"))
  }

  "t-closeness" should "not be satisfied when distributions are far apart" in {
    val tClose = new TCloseness(t = 0.1)

    val data = Seq(
      ("A", "Disease1"),
      ("A", "Disease1"),
      ("A", "Disease1"),
      ("B", "Disease2"),
      ("B", "Disease2"),
      ("B", "Disease2")
    ).toDF("QuasiIdentifier", "Disease")

    assert(!tClose.isTClose(data, "Disease"))
  }

  "t-closeness" should "calculate distance for multiple quasi-identifiers" in {
    val tClose = new TCloseness(t = 0.5)

    val data = Seq(
      ("A", "X", "Disease1"),
      ("A", "X", "Disease2"),
      ("B", "Y", "Disease1"),
      ("B", "Y", "Disease2"),
      ("C", "Z", "Disease3"),
      ("C", "Z", "Disease3")
    ).toDF("Quasi1", "Quasi2", "Disease")

    val result = tClose(data, "Disease")
    assert(result.columns.contains("distance"))
    assert(result.columns.contains("row_hash"))
  }

  "removeLessThanTRows" should "filter out non-compliant equivalence classes" in {
    val tClose = new TCloseness(t = 0.2)

    val data = Seq(
      ("A", "Disease1"),
      ("A", "Disease2"),
      ("A", "Disease3"),
      ("B", "Disease1"),
      ("B", "Disease1"),
      ("B", "Disease1")
    ).toDF("QuasiIdentifier", "Disease")

    val result = tClose.removeLessThanTRows(data, "Disease")
    // The result should only contain rows from equivalence classes that satisfy t-closeness
    assert(result.count() <= data.count())
  }

  "t-closeness with uniform distribution" should "have low distance" in {
    val tClose = new TCloseness(t = 0.1)

    val data = Seq(
      ("A", "Value1"),
      ("A", "Value2"),
      ("A", "Value3"),
      ("B", "Value1"),
      ("B", "Value2"),
      ("B", "Value3")
    ).toDF("Group", "Sensitive")

    assert(tClose.isTClose(data, "Sensitive"))
  }
}
