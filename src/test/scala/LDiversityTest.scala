import org.mitchelllisle.ldiversity.LDiversity
import org.scalatest.flatspec.AnyFlatSpec

import org.apache.spark.sql.{functions => F}

class LDiversityTest extends AnyFlatSpec with SparkFunSuite {

  import spark.implicits._

  "Running L-Diversity" should "correctly count groups" in {
    val lDiv = new LDiversity(l = 3)
    val output = lDiv(sampleNetflixData.drop("date"), "user_id")
    assert(output.count() == 40)
  }

  "Removing rows" should "not include non LDiverse rows" in {
    val lDiv = new LDiversity(l = 7)
    val output = lDiv.removeLessThanLRows(sampleNetflixData.drop("date"), "user_id")
    assert(output.filter(F.col("distinctCount") < 7).count() == 0)
  }

  "Data" should "meet l-diversity requirements" in {
    val lDiv = new LDiversity(l = 2)

    val data = Seq(
      ("A", "Male"),
      ("A", "Female"),
      ("B", "Male"),
      ("B", "Other")
    ).toDF("QuasiIdentifier", "SensitiveAttribute")

    assert(lDiv.isLDiverse(data, "SensitiveAttribute"))
  }

  "Data" should "not meet l-diversity requirements" in {
    val lDiv = new LDiversity(l = 2)

    val data = Seq(
      ("A", "Male"),
      ("A", "Male"),
      ("B", "Female"),
      ("B", "Other")
    ).toDF("QuasiIdentifier", "SensitiveAttribute")

    assert(!lDiv.isLDiverse(data, "SensitiveAttribute"))
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

    assert(!lDiv.isLDiverse(data, "SensitiveAttribute"))
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

    assert(!lDiv.isLDiverse(data, "SensitiveAttribute"))
  }

}
