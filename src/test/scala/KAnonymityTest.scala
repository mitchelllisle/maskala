import org.mitchelllisle.kanonymity.KAnonymity
import scala.collection.immutable.Seq

class KAnonymityTest extends SparkFunSuite {
  import spark.implicits._

  test("isKAnonymous returns true for k-anonymous data") {
    val data = Seq(
      ("1234", "Male"),
      ("1234", "Male"),
      ("1236", "Female"),
      ("1236", "Female")
    ).toDF("ID", "Gender")

    val kAnon = new KAnonymity(2)
    assert(kAnon.evaluate(data))
  }

  test("isKAnonymous returns false for non k-anonymous data") {

    val data = Seq(
      ("1234", "Male"),
      ("1235", "Male"),
      ("1236", "Female"),
      ("1237", "Male")
    )

    val df = data.toDF("ID", "Gender")

    val kAnon = new KAnonymity(2)
    assert(!kAnon.evaluate(df))
  }

  test("filterKAnonymous filters out rows not meeting the k-anonymity threshold") {
    val data = Seq(
      ("1234", "Male"),
      ("1234", "Male"),
      ("1235", "Male"),
      ("1236", "Female")
    ).toDF("ID", "Gender")

    val kAnon = new KAnonymity(2)
    val result = kAnon.filter(data)
    val expected = Seq(
      ("1234", "Male"),
      ("1234", "Male")
    ).toDF("ID", "Gender")

    assert(result.except(expected).count() == 0 && expected.except(result).count() == 0)
  }

  test("filterKAnonymous retains all rows when they all meet the k-anonymity threshold") {
    val data = Seq(
      ("1234", "Male"),
      ("1234", "Male"),
      ("1235", "Male"),
      ("1235", "Male")
    ).toDF("ID", "Gender")

    val kAnon = new KAnonymity(2)
    val result = kAnon.filter(data)

    assert(result.except(data).count() == 0 && data.except(result).count() == 0)
  }

  test("filterKAnonymous handles ignoring specified columns") {
    val data = Seq(
      ("1234", "Male", "user1"),
      ("1234", "Male", "user2"),
      ("1235", "Male", "user3"),
      ("1235", "Male", "user4")
    ).toDF("Dept", "Gender", "UserID")

    val kAnon = new KAnonymity(2)
    val result = kAnon.filter(data, Seq("UserID"))

    assert(result.except(data).count() == 0 && data.except(result).count() == 0)
  }
}
