import org.apache.spark.sql.{AnalysisException, DataFrame, SaveMode, SparkSession}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec

import scala.util.Try


class SparkFunSuite extends AnyFlatSpec with BeforeAndAfterAll {
  val spark: SparkSession = SparkSession
    .builder
    .appName("MaskalaTests")
    .master("local[*]")
    .getOrCreate()

  val netflixSchema = "netflix"
  val netflixRatingsTable = "ratings"

  protected override def beforeAll(): Unit = {
    dropDatabase()

    val sampleNetflixData: DataFrame = spark
      .read
      .option("header", "true")
      .csv("src/test/resources/netflix-sample.csv")

    // Spark is inconsistent in when it's cleaning up resources; this is here to ignore errors when trying to create
    // a database that already exists
    Try {
      spark.sql(s"CREATE DATABASE IF NOT EXISTS $netflixSchema")
      sampleNetflixData.write.mode(SaveMode.Overwrite).saveAsTable(s"$netflixSchema.$netflixRatingsTable")
    } recover {
      case err: AnalysisException if err.message.contains("LOCATION_ALREADY_EXISTS") =>
        println(s"$netflixSchema.$netflixRatingsTable already exists. continuing")
    }

    super.beforeAll()
  }

  def dropDatabase(): Unit = {
    spark.sql(s"DROP SCHEMA IF EXISTS $netflixSchema CASCADE")
  }

  protected override def afterAll(): Unit = {
    dropDatabase()
    super.afterAll()
  }
}
