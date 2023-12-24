import org.mitchelllisle.anonymisers.{DateParams, DateStrategy}
import org.scalatest.flatspec.AnyFlatSpec


class DateGeneralisationTest extends AnyFlatSpec with SparkFunSuite {
  import spark.implicits._

  private val data = Seq("2023-08-12", "2023-02-10", "2024-11-20").toDF("Date")

  "YearOnly" should "be generalised on a date column" in {
    val strategy = DateStrategy("Date")
    val params = DateParams("yyyy")

    val generalizedData = strategy(data, params)
    val results = generalizedData.collect().map(row => row.getString(0))

    assert(results sameElements Array("2023", "2023", "2024"))
  }

  "MonthlyOnly" should "be generalised on a date column" in {
    val strategy = DateStrategy("Date")
    val params = DateParams("yyyy-MM")

    val generalizedData = strategy(data, params)
    val results = generalizedData.collect().map(row => row.getString(0))

    assert(results sameElements Array("2023-08", "2023-02", "2024-11"))
  }

  "DateLevel" should "be generalised on a date column" in {
    val strategy = DateStrategy("Date")
    val params = DateParams("d-MMM-yyyy")

    val generalizedData = strategy(data, params)
    val results = generalizedData.collect().map(row => row.getString(0))

    assert(results sameElements Array("12-Aug-2023", "10-Feb-2023", "20-Nov-2024"))
  }
}
