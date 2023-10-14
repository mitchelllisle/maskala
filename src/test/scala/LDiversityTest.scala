import org.mitchelllisle.ldiversity.LDiversity

class LDiversityTest extends SparkFunSuite {

  import spark.implicits._

  test("Data meets l-diversity requirements") {
    val lDiv = new LDiversity(l = 2)

    val data = Seq(
      ("A", "Male"),
      ("A", "Female"),
      ("B", "Male"),
      ("B", "Other")
    ).toDF("QuasiIdentifier", "SensitiveAttribute")

    assert(lDiv.evaluate(data, "SensitiveAttribute"))
  }

  test("Data does not meet l-diversity requirements") {
    val lDiv = new LDiversity(l = 2)

    val data = Seq(
      ("A", "Male"),
      ("A", "Male"),
      ("B", "Female"),
      ("B", "Other")
    ).toDF("QuasiIdentifier", "SensitiveAttribute")

    assert(!lDiv.evaluate(data, "SensitiveAttribute"))
  }

  test("l-diversity for multiple quasi-identifiers") {
    val lDiv = new LDiversity(l = 2)

    val data = Seq(
      ("A", "X", "Male"),
      ("A", "X", "Female"),
      ("B", "Y", "Male"),
      ("B", "Y", "Other"),
      ("A", "Z", "Male"),
      ("A", "Z", "Male")
    ).toDF("Quasi1", "Quasi2", "SensitiveAttribute")

    assert(!lDiv.evaluate(data, "SensitiveAttribute"))
  }

  test("l-diversity with larger l requirement") {
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

    assert(!lDiv.evaluate(data, "SensitiveAttribute"))
  }

}
