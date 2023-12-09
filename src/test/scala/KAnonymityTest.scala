import org.mitchelllisle.kanonymity.KAnonymity

class KAnonymityTest extends SparkFunSuite {
  import spark.implicits._

  "isKAnonymous" should "return true for k-anonymous data" in {
    val data = Seq(
      ("1234", "Male"),
      ("1234", "Male"),
      ("1236", "Female"),
      ("1236", "Female")
    ).toDF("ID", "Gender")

    val kAnon = new KAnonymity(2)
    assert(kAnon.isKAnonymous(data))
  }

  "isKAnonymous" should "return false for non k-anonymous data" in {

    val data = Seq(
      ("1234", "Male"),
      ("1235", "Male"),
      ("1236", "Female"),
      ("1237", "Male")
    )

    val df = data.toDF("ID", "Gender")

    val kAnon = new KAnonymity(2)
    assert(!kAnon.isKAnonymous(df))
  }

  "filterKAnonymous" should "filter out rows not meeting the k-anonymity threshold" in {
    val data = Seq(
      ("1234", "Male"),
      ("1234", "Male"),
      ("1235", "Male"),
      ("1236", "Female")
    ).toDF("ID", "Gender")

    val kAnon = new KAnonymity(2)
    val result = kAnon.removeLessThanKRows(data)
    val expected = Seq(
      ("1234", "Male"),
      ("1234", "Male")
    ).toDF("ID", "Gender")

    assert(result.except(expected).count() == 0 && expected.except(result).count() == 0)
  }

  "filterKAnonymous" should "retain all rows when they all meet the k-anonymity threshold" in {
    val data = Seq(
      ("1234", "Male"),
      ("1234", "Male"),
      ("1235", "Male"),
      ("1235", "Male")
    ).toDF("ID", "Gender")

    val kAnon = new KAnonymity(2)
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

    val kAnon = new KAnonymity(2)

    val columns = data.columns.filterNot(_.equals("UserID"))
    val result = kAnon.removeLessThanKRows(data, Some(columns))

    assert(result.except(data).count() == 0 && data.except(result).count() == 0)
  }
}
