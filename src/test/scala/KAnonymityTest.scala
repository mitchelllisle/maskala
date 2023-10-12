import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import KAnonymity._
import org.scalatest.funsuite.AnyFunSuite
import scala.collection.immutable.Seq

class KAnonymityTest extends AnyFunSuite with BeforeAndAfterAll {
  val spark: SparkSession = SparkSession.builder()
    .appName("KAnonymityTests")
    .master("local[*]")
    .getOrCreate()

  override def afterAll(): Unit = {
    if (spark != null) {
      spark.stop()
    }
  }

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
      ("1234", "Male", "New York"),
      ("1234", "Male", "Los Angeles"),
      ("1235", "Male", "Boston"),
      ("1235", "Male", "Chicago")
    ).toDF("ID", "Gender", "City")

    // Here we ignore the "City" column when determining k-anonymity
    val result = filterKAnonymous(data, 2, Seq("City"))
    val expected = data // The expected output remains unchanged since the "City" column is ignored

    assert(result.except(expected).count() == 0 && expected.except(result).count() == 0)
  }

}
