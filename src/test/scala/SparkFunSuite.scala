import org.scalatest.funsuite.AnyFunSuite
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.scalactic.source.Position
import org.scalatest.BeforeAndAfter

import scala.reflect.io.Directory
import java.io.File


trait SparkFunSuite extends AnyFunSuite {
  val spark: SparkSession = SparkSession.builder()
    .appName("MaskalaTests")
    .master("local[*]")
    .getOrCreate()

  val netflixSchema = "netflix"
  val netflixRatingsTable = "ratings"

  val sampleNetflixData: DataFrame = spark
    .read
    .option("header", "true")
    .csv("src/test/resources/netflix-sample.csv")

  spark.sql(s"CREATE DATABASE IF NOT EXISTS $netflixSchema")
  sampleNetflixData.write.mode(SaveMode.Ignore).saveAsTable(s"$netflixSchema.$netflixRatingsTable")
}
