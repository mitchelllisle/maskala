import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import KAnonymity._
import org.scalatest.funsuite.AnyFunSuite

class KAnonymityTest extends AnyFunSuite with BeforeAndAfterAll {
  val spark: SparkSession = SparkSession.builder()
    .appName("KAnonymityTests")
    .master("local[*]") // Use local mode for tests
    .getOrCreate()

  override def afterAll(): Unit = {
    if (spark != null) {
      spark.stop()
    }
  }

  test("isKAnonymous returns true for k-anonymous data") {
    import spark.implicits._

    val data = Seq(
      ("1234", "Male"),
      ("1234", "Male"),
      ("1236", "Female"),
      ("1236", "Female")
    )

    val df = data.toDF("ID", "Gender")
    assert(isKAnonymous(df, 2))
  }

  test("isKAnonymous returns false for non k-anonymous data") {
    import spark.implicits._

    val data = Seq(
      ("1234", "Male"),
      ("1235", "Male"),
      ("1236", "Female"),
      ("1237", "Male")
    )

    val df = data.toDF("ID", "Gender")
    assert(!isKAnonymous(df, 2))
  }

}