import org.mitchelllisle.anonymisers.{MappingParams, MappingStrategy}
import org.scalatest.flatspec.AnyFlatSpec


class MappingGeneralisationTest extends AnyFlatSpec with SparkFunSuite {
  import spark.implicits._

  "MappingGeneralisation" should "correctly map values based on provided mapping" in {
    val data = Seq("A", "B", "C", "D", "E").toDF("Letters")

    val mapping = "A=X,B=Y,C=Z"
    val strategy = MappingStrategy("Letters")

    val generalizedData = strategy(data, MappingParams(mapping))
    val results = generalizedData.collect().map(row => row.getString(0))

    assert(results sameElements Array("X", "Y", "Z", "D", "E"))
  }
}
