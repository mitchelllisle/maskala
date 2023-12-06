import org.mitchelllisle.generaliser.MappingGeneralisation
import org.scalatest.BeforeAndAfterAll

import scala.collection.immutable.Seq

class MappingGeneralisationTest extends SparkFunSuite {
  import spark.implicits._

  "MappingGeneralisation" should "correctly map values based on provided mapping" in {
    val data = Seq("A", "B", "C", "D", "E").toDF("Letters")

    val mapping = Map("A" -> "X", "B" -> "Y", "C" -> "Z")
    val strategy = MappingGeneralisation("Letters", mapping)

    val generalizedData = strategy(data)
    val results = generalizedData.collect().map(row => row.getString(0))

    assert(results sameElements Array("X", "Y", "Z", "D", "E"))
  }
}
