import org.apache.spark.sql.DataFrame
import org.mitchelllisle.analysers.KAnonymity
import org.scalatest.flatspec.AnyFlatSpec

class KAnonymityTest extends AnyFlatSpec with SparkFunSuite {
  import spark.implicits._

  val data: DataFrame = Seq(
    ("30", "Male"),
    ("30", "Male"),
    ("27", "Female"),
    ("45", "Female")
  ).toDF("Age", "Gender")

  val kAnon = new KAnonymity(2)

  "isKAnonymous" should "return true for k-anonymous data" in {
    val data = Seq(
      ("1234", "Male"),
      ("1234", "Male"),
      ("1236", "Female"),
      ("1236", "Female")
    ).toDF("ID", "Gender")

    assert(kAnon.isKAnonymous(data))
  }

  "isKAnonymous" should "return false for non k-anonymous data" in {

    val allUniqueData = Seq(
      ("1234", "Male"),
      ("1235", "Male"),
      ("1236", "Female"),
      ("1237", "Male")
    ).toDF("ID", "Gender")

    assert(!kAnon.isKAnonymous(allUniqueData))
  }
}
