import org.scalatest.funsuite.AnyFunSuite
import org.apache.spark.sql.SparkSession



trait SparkFunSuite extends AnyFunSuite {
  val spark: SparkSession = SparkSession.builder()
    .appName("MaskalaTests")
    .master("local[*]")
    .getOrCreate()
}
