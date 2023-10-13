import kanonymity.KAnonymity._
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

    isKAnonymous(data, 2) match {
      case Left(error) => fail(s"expected success but got error: $error")
      case Right(value) => assert(value)
    }
  }

  test("isKAnonymous returns false for non k-anonymous data") {

    val data = Seq(
      ("1234", "Male"),
      ("1235", "Male"),
      ("1236", "Female"),
      ("1237", "Male")
    )

    val df = data.toDF("ID", "Gender")
    isKAnonymous(df, 2) match {
      case Left(error) => fail(s"expected success but received error: $error")
      case Right(value) => assert(!value)
    }
  }

  test("k less than one must error") {
    val data = Seq(("1234", "Male")).toDF()
    isKAnonymous(data, -1) match {
      case Left(error) => assert(error.nonEmpty)
      case _ => fail("this test was supposed to throw a Left(error)")
    }
  }

  test("filterKAnonymous filters out rows not meeting the k-anonymity threshold") {
    val data = Seq(
      ("1234", "Male"),
      ("1234", "Male"),
      ("1235", "Male"),
      ("1236", "Female")
    ).toDF("ID", "Gender")

    val result = filterKAnonymous(data, 2)
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

    val result = filterKAnonymous(data, 2)

    assert(result.except(data).count() == 0 && data.except(result).count() == 0)
  }

  test("filterKAnonymous handles ignoring specified columns") {
    val data = Seq(
      ("1234", "Male", "user1"),
      ("1234", "Male", "user2"),
      ("1235", "Male", "user3"),
      ("1235", "Male", "user4")
    ).toDF("Dept", "Gender", "UserID")

    // Here we ignore the "UserId" column when determining k-anonymity
    val result = filterKAnonymous(data, 2, Seq("UserID"))

    assert(result.except(data).count() == 0 && data.except(result).count() == 0)
  }
}
