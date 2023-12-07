import org.mitchelllisle.generalisation.{CustomLevel, DateGeneralisation, MonthYear, QuarterYear, YearOnly}


class DateGeneralisationTest extends SparkFunSuite {
  import spark.implicits._

  "YearOnly" should "be generalised on a date column" in {
    val data = Seq("2023-08-12", "2023-02-10", "2024-11-20").toDF("Date")
    val strategy = DateGeneralisation("Date", YearOnly)

    val generalizedData = strategy(data)
    val results = generalizedData.collect().map(row => row.getString(0))

    assert(results sameElements Array("2023", "2023", "2024"))
  }

  "MonthlyOnly" should "be generalised on a date column" in {
    val data = Seq("2023-08-12", "2023-02-10", "2024-11-20").toDF("Date")
    val strategy = DateGeneralisation("Date", MonthYear)

    val generalizedData = strategy(data)
    val results = generalizedData.collect().map(row => row.getString(0))

    assert(results sameElements Array("2023-08", "2023-02", "2024-11"))
  }

  "QuarterYear" should "be generalised on a date column" in {
    val data = Seq("2023-08-12", "2023-02-10", "2024-11-20").toDF("Date")
    val strategy = DateGeneralisation("Date", QuarterYear)

    val generalizedData = strategy(data)
    val results = generalizedData.collect().map(row => row.getString(0))

    assert(results sameElements Array("3-2023", "1-2023", "4-2024"))
  }

  "CustomLevel" should "be generalised on a date column" in {
    val data = Seq("2023-08-12", "2023-02-10", "2024-11-20").toDF("Date")
    val customFormat = "d-MMM-yyyy"
    val strategy = DateGeneralisation("Date", CustomLevel(customFormat))

    val generalizedData = strategy(data)
    val results = generalizedData.collect().map(row => row.getString(0))

    assert(results sameElements Array("12-Aug-2023", "10-Feb-2023", "20-Nov-2024"))
  }
}
