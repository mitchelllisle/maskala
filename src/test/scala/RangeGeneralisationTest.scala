import org.mitchelllisle.anonymisers.{RangeParams, RangeStrategy}
import org.scalatest.flatspec.AnyFlatSpec

class RangeGeneralisationTest extends AnyFlatSpec with SparkFunSuite {
  import spark.implicits._

  "RangeGeneralization" should  "correctly generalize numeric column values" in {
    val data = Seq(1, 5, 13, 15, 29, 30, 35).toDF("Numbers")
    val strategy = RangeStrategy("Numbers")
    val params = RangeParams()

    val generalizedData = strategy(data, params)
    val results = generalizedData.collect().map(row => row.getString(0))

    assert(results sameElements Array("0-9", "0-9", "10-19", "10-19", "20-29", "30-39", "30-39"))
  }

  "RangeGeneralization" should "handle negative values correctly, with separator" in {
    val data = Seq(-5, -2, 0, 3, 12).toDF("Numbers")
    val strategy = RangeStrategy("Numbers")
    val params = RangeParams(10, ":")

    val generalizedData = strategy(data, params)
    val results = generalizedData.collect().map(row => row.getString(0))

    assert(results sameElements Array("-10:-1", "-10:-1", "0:9", "0:9", "10:19"))
  }

  "RangeGeneralization" should "handles empty DataFrame" in {
    val data = Seq.empty[Int].toDF("Numbers")
    val strategy = RangeStrategy("Numbers")
    val params = RangeParams()

    val generalizedData = strategy(data, params)
    assert(generalizedData.count() === 0)
  }
}
