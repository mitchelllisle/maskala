import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec

import scala.reflect.io.Directory
import java.io.File


class SparkFunSuite extends AnyFlatSpec with BeforeAndAfterAll {
  val spark: SparkSession = SparkSession
    .builder
    .appName("MaskalaTests")
    .master("local[*]")
    .getOrCreate()

  val netflixSchema = "netflix"
  val netflixRatingsTable = "ratings"

  protected override def beforeAll(): Unit = {
    removeDataDir()

    val sampleNetflixData: DataFrame = spark
      .read
      .option("header", "true")
      .csv("src/test/resources/netflix-sample.csv")

    spark.sql(s"CREATE DATABASE IF NOT EXISTS $netflixSchema")
    sampleNetflixData.write.mode(SaveMode.Overwrite).saveAsTable(s"$netflixSchema.$netflixRatingsTable")
    super.beforeAll()
  }

  def removeDataDir(): Unit = {
    val dir = new Directory(new File("spark-warehouse"))
    dir.deleteRecursively()
  }
}
