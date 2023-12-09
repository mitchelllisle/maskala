import org.apache.spark.sql.DataFrame
import org.mitchelllisle.kanonymity.KAnonymity
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

  "filterKAnonymous" should "filter out rows not meeting the k-anonymity threshold" in {

    val result = kAnon.removeLessThanKRows(data)
    val expected = Seq(
      ("30", "Male"),
      ("30", "Male")
    ).toDF("Age", "Gender")

    assert(result.except(expected).count() == 0 && expected.except(result).count() == 0)
  }

  "filterKAnonymous" should "retain all rows when they all meet the k-anonymity threshold" in {
    val data = Seq(
      ("1234", "Male"),
      ("1234", "Male"),
      ("1235", "Male"),
      ("1235", "Male")
    ).toDF("ID", "Gender")

    val result = kAnon.removeLessThanKRows(data)

    assert(result.except(data).count() == 0 && data.except(result).count() == 0)
  }

  "filterKAnonymous" should "handles ignoring specified columns" in {
    val data = Seq(
      ("1234", "Male", "user1"),
      ("1234", "Male", "user2"),
      ("1235", "Male", "user3"),
      ("1235", "Male", "user4")
    ).toDF("Dept", "Gender", "UserID")


    val columns = data.columns.filterNot(_.equals("UserID"))
    val result = kAnon.removeLessThanKRows(data, Some(columns))

    assert(result.except(data).count() == 0 && data.except(result).count() == 0)
  }

}
