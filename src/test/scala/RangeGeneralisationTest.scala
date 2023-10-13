import kanonymity.KAnonymity
import kanonymity.generalisation.{MappingGeneralisation, RangeGeneralisation}
import scala.collection.immutable.Seq

class RangeGeneralisationTest extends SparkFunSuite {
  import spark.implicits._

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

    val kAnon = new KAnonymity(2)
    val result = kAnon.generalise(data, Seq(strategy)).collect().map(row => row.getString(0))
    assert(result sameElements Array("0-4", "0-4", "0-4", "0-4", "5-9", "5-9", "5-9", "5-9", "5-9", "10-14"))
  }
}
