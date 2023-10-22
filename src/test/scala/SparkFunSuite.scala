import org.apache.spark.sql.SparkSession
import org.scalatest.flatspec.AnyFlatSpec


class SparkFunSuite extends AnyFlatSpec {
  val spark: SparkSession = SparkSession
    .builder
    .appName("MaskalaTests")
    .master("local[*]")
    .getOrCreate()
}
