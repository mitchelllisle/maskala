import org.mitchelllisle.anonymisers.generalisation.{Generaliser, RangeGeneralisation}
import org.scalatest.flatspec.AnyFlatSpec

class RangeGeneralisationTest extends AnyFlatSpec with SparkFunSuite {
  import spark.implicits._

  "RangeGeneralization" should  "correctly generalize numeric column values" in {
    val data = Seq(1, 5, 13, 15, 29, 30, 35).toDF("Numbers")
    val strategy = RangeGeneralisation("Numbers", 10)

    val generalizedData = strategy(data)
    val results = generalizedData.collect().map(row => row.getString(0))

    assert(results sameElements Array("0-9", "0-9", "10-19", "10-19", "20-29", "30-39", "30-39"))
  }

  "RangeGeneralization" should "handle negative values correctly, with separator" in {
    val data = Seq(-5, -2, 0, 3, 12).toDF("Numbers")
    val strategy = RangeGeneralisation("Numbers", 10, ":")

    val generalizedData = strategy(data)
    val results = generalizedData.collect().map(row => row.getString(0))

    assert(results sameElements Array("-10:-1", "-10:-1", "0:9", "0:9", "10:19"))
  }

  "RangeGeneralization" should "handles empty DataFrame" in {
    val data = Seq.empty[Int].toDF("Numbers")
    val strategy = RangeGeneralisation("Numbers", 10)

    val generalizedData = strategy(data)
    assert(generalizedData.count() === 0)
  }

  "RangeGeneralisation" should "work from KAnonymity object" in {
    val data = Seq(1, 2, 3,4 ,5 ,6 ,7 ,8 ,9, 10).toDF("Numbers")
    val strategy = RangeGeneralisation("Numbers", 5)

    val generaliser = new Generaliser(Seq(strategy))
    val result = generaliser.apply(data).collect().map(row => row.getString(0))
    assert(result sameElements Array("0-4", "0-4", "0-4", "0-4", "5-9", "5-9", "5-9", "5-9", "5-9", "10-14"))
  }
}
