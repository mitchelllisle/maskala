import org.scalatest.flatspec.AnyFlatSpec

import org.mitchelllisle.analysers.LDiversity

class LDiversityTest extends AnyFlatSpec with SparkFunSuite {

  import spark.implicits._

  "Running L-Diversity" should "correctly count groups" in {
    val lDiv = new LDiversity(l = 3)
    val output = lDiv(sampleNetflixData.drop("date", "location"), "rating", "user_id")
    assert(output.count() == 8)
  }

  "Data" should "meet l-diversity requirements" in {
    val lDiv = new LDiversity(l = 2)

    val data = Seq(
      ("1", "A", "Male"),
      ("2", "A", "Female"),
      ("3", "B", "Male"),
      ("4", "B", "Other")
    ).toDF("UserId", "QuasiIdentifier", "SensitiveAttribute")

    assert(lDiv.isLDiverse(data, "SensitiveAttribute", "UserId"))
  }

  "Data" should "not meet l-diversity requirements" in {
    val lDiv = new LDiversity(l = 2)

    val data = Seq(
      ("1", "A", "Male"),
      ("2", "A", "Male"),
      ("3", "B", "Female"),
      ("4", "B", "Other")
    ).toDF("UserId", "QuasiIdentifier", "SensitiveAttribute")

    assert(!lDiv.isLDiverse(data, "SensitiveAttribute", "UserId"))
  }

  "l-diversity"  should "match for multiple quasi-identifiers" in {
    val lDiv = new LDiversity(l = 2)

    val data = Seq(
      ("1", "A", "X", "Male"),
      ("3", "A", "X", "Female"),
      ("3", "B", "Y", "Male"),
      ("4", "B", "Y", "Other"),
      ("5", "A", "Z", "Male"),
      ("6", "A", "Z", "Male")
    ).toDF("UserId", "Quasi1", "Quasi2", "SensitiveAttribute")

    assert(!lDiv.isLDiverse(data, "SensitiveAttribute", "UserId"))
  }

  "l-diversity with larger l requirement" should "not be met" in {
    val lDiv = new LDiversity(l = 4)

    val data = Seq(
      ("1", "A", "X", "Male"),
      ("2", "A", "X", "Female"),
      ("3", "A", "X", "Other"),
      ("4", "B", "Y", "Male"),
      ("5", "B", "Y", "Other"),
      ("6", "A", "Z", "Male"),
      ("7", "A", "Z", "Male")
    ).toDF("UserId", "Quasi1", "Quasi2", "SensitiveAttribute")

    assert(!lDiv.isLDiverse(data, "SensitiveAttribute", "UserId"))
  }
}
