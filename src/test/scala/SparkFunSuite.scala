import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.flatspec.AnyFlatSpec


class SparkFunSuite extends AnyFlatSpec {

  val spark: SparkSession = SparkSession
    .builder
    .appName("MaskalaTests")
    .master("local[*]")
    .getOrCreate()

  val sampleNetflixData: DataFrame = spark
    .read
    .option("header", "true")
    .csv("src/test/resources/netflix.csv")
}
