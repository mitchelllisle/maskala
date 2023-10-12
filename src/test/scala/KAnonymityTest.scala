import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import kanonymity.KAnonymity._
import kanonymity.{RangeGeneralisation, MappingGeneralisation}
import org.scalatest.funsuite.AnyFunSuite
import scala.collection.immutable.Seq

class KAnonymityTest extends AnyFunSuite with BeforeAndAfterAll {
  val spark: SparkSession = SparkSession.builder()
    .appName("KAnonymityTests")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  override def afterAll(): Unit = {
    if (spark != null) {
      spark.stop()
    }
  }

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

  test("RangeGeneralization correctly generalizes numeric column values") {
    val data = Seq(1, 5, 13, 15, 29, 30, 35).toDF("Numbers")
    val strategy = RangeGeneralisation("Numbers", 10)

    val generalizedData = strategy(data)
    val results = generalizedData.collect().map(row => row.getString(0))

    assert(results sameElements Array("0-9", "0-9", "10-19", "10-19", "20-29", "30-39", "30-39"))
  }

  test("RangeGeneralization handles negative values correctly, with separator") {
    val data = Seq(-5, -2, 0, 3, 12).toDF("Numbers")
    val strategy = RangeGeneralisation("Numbers", 10, ":")

    val generalizedData = strategy(data)
    val results = generalizedData.collect().map(row => row.getString(0))

    assert(results sameElements Array("-10:-1", "-10:-1", "0:9", "0:9", "10:19"))
  }

  test("RangeGeneralization handles empty DataFrame") {
    val data = Seq.empty[Int].toDF("Numbers")
    val strategy = RangeGeneralisation("Numbers", 10)

    val generalizedData = strategy(data)
    assert(generalizedData.count() === 0)
  }

  test("RangeGeneralisation from KAnonymity object") {
    val data = Seq(1, 2, 3,4 ,5 ,6 ,7 ,8 ,9, 10).toDF("Numbers")
    val strategy = RangeGeneralisation("Numbers", 5)

    val result = generalise(data, Seq(strategy)).collect().map(row => row.getString(0))
    assert(result sameElements Array("0-4", "0-4", "0-4", "0-4", "5-9", "5-9", "5-9", "5-9", "5-9", "10-14"))
  }

  test("MappingGeneralisation correctly maps values based on provided mapping") {
    val data = Seq("A", "B", "C", "D", "E").toDF("Letters")

    val mapping = Map("A" -> "X", "B" -> "Y", "C" -> "Z")
    val strategy = MappingGeneralisation("Letters", mapping)

    val generalizedData = strategy(data)
    val results = generalizedData.collect().map(row => row.getString(0))

    assert(results sameElements Array("X", "Y", "Z", "D", "E"))
  }

}
