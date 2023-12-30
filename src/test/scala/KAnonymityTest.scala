import org.apache.spark.sql.DataFrame
import org.mitchelllisle.analysers.KAnonymity
import org.scalatest.flatspec.AnyFlatSpec

class KAnonymityTest extends AnyFlatSpec with SparkFunSuite {
  import spark.implicits._

  val data: DataFrame = Seq(
    ("1234", "30", "Male"),
    ("1235", "30", "Male"),
    ("1236", "27", "Female"),
    ("1237", "27", "Female")
  ).toDF("ID", "Age", "Gender")

  val kAnon = new KAnonymity(2)

  "isKAnonymous" should "return true for k-anonymous data" in {
    // excluding the ID column should result in k-anonymous data
    assert(kAnon.isKAnonymous(data, Some("ID")))
  }

  "isKAnonymous" should "return false for non k-anonymous data" in {
    // including the ID column should result in k-anonymous data
    assert(!kAnon.isKAnonymous(data, None))
  }

  "apply" should "return a DataFrame with a row_hash column" in {
    val kData = kAnon(data, Some("ID"))
    assert(kData.columns.contains("row_hash"))
  }

}
