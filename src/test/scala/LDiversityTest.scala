import org.mitchelllisle.ldiversity.LDiversity

class LDiversityTest extends SparkFunSuite {

  import spark.implicits._

  "Data" should "meet l-diversity requirements" in {
    val lDiv = new LDiversity(l = 2)

    val data = Seq(
      ("A", "Male"),
      ("A", "Female"),
      ("B", "Male"),
      ("B", "Other")
    ).toDF("QuasiIdentifier", "SensitiveAttribute")

    assert(lDiv.apply(data, "SensitiveAttribute"))
  }

  "Data" should "not meet l-diversity requirements" in {
    val lDiv = new LDiversity(l = 2)

    val data = Seq(
      ("A", "Male"),
      ("A", "Male"),
      ("B", "Female"),
      ("B", "Other")
    ).toDF("QuasiIdentifier", "SensitiveAttribute")

    assert(!lDiv.apply(data, "SensitiveAttribute"))
  }

  "l-diversity"  should "match for multiple quasi-identifiers" in {
    val lDiv = new LDiversity(l = 2)

    val data = Seq(
      ("A", "X", "Male"),
      ("A", "X", "Female"),
      ("B", "Y", "Male"),
      ("B", "Y", "Other"),
      ("A", "Z", "Male"),
      ("A", "Z", "Male")
    ).toDF("Quasi1", "Quasi2", "SensitiveAttribute")

    assert(!lDiv.apply(data, "SensitiveAttribute"))
  }

  "l-diversity with larger l requirement" should "not be met" in {
    val lDiv = new LDiversity(l = 4)

    val data = Seq(
      ("A", "X", "Male"),
      ("A", "X", "Female"),
      ("A", "X", "Other"),
      ("B", "Y", "Male"),
      ("B", "Y", "Other"),
      ("A", "Z", "Male"),
      ("A", "Z", "Male")
    ).toDF("Quasi1", "Quasi2", "SensitiveAttribute")

    assert(!lDiv.apply(data, "SensitiveAttribute"))
  }

}
