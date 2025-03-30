import org.apache.spark.sql.{DataFrame, SparkSession}

trait SparkFunSuite {
  lazy val spark: SparkSession = {
    SparkSession
      .builder()
      .appName("SparkTest")
      .master("local[*]")
      .getOrCreate()
  }
  lazy val sampleNetflixData: DataFrame = spark
    .read
    .option("header", "true")
    .csv("src/test/resources/netflix.csv")
}
