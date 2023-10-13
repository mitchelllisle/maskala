import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.apache.spark.sql.SparkSession

trait SparkFunSuite extends AnyFunSuite with BeforeAndAfterAll {
  val spark: SparkSession = SparkSession.builder()
    .appName("MaskalaTests")
    .master("local[*]")
    .getOrCreate()

  override def afterAll(): Unit = {
    if (spark != null) {
      spark.stop()
    }
  }
}
