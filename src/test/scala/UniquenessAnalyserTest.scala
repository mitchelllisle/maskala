import org.mitchelllisle.khyperloglog.{KHyperLogLogAnalyser, UniquenessAnalyser}
import org.apache.spark.sql.{AnalysisException, DataFrame}
import org.scalatest.BeforeAndAfterAll

import scala.util.Try


class UniquenessAnalyserTest extends SparkFunSuite with BeforeAndAfterAll {
  val k = 2056
  val khll = new KHyperLogLogAnalyser(spark, k = k)
  val analyser = new UniquenessAnalyser(spark)

  import spark.implicits._

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
      sampleNetflixData.write.saveAsTable(s"$netflixSchema.$netflixRatingsTable")
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

  def getNetflixRatings: DataFrame = {
    analyser.getTable("netflix", "ratings", "customerId", Seq("rating"))
  }

  "Getting table" should "return a non empty dataframe" in {
    assert(!getNetflixRatings.isEmpty)
  }

  "Uniqueness data" should "match what is expected" in {
    val expected = Seq((3, 2427), (5, 2175), (1, 1108), (4, 2905), (2, 1243)).toDF()
    val unique = analyser.uniquenessData(getNetflixRatings)
    assert(unique.except(expected).count() == 0)
  }

  "Counting" should "equal number of non-null records" in {
    assert(analyser.numValues(getNetflixRatings) == 9992)
  }

  "Distribution" should "match expected values" in {
    val expected = Seq((2427, 1, 0.2), (2175, 1, 0.2), (1108, 1, 0.2), (2905, 1, 0.2), (1243, 1, 0.2)).toDF()

    val unique = analyser.uniquenessData(getNetflixRatings)
    val numValues = analyser.numValues(unique)
    val distribution = analyser.uniquenessDistribution(unique, numValues)

    assert(distribution.except(expected).count() == 0)
  }

  "Running pipeline" should "produce uniqueness results" in {
    val uniqueness = analyser.run(getNetflixRatings)
    println("")
  }

  "hashIDCol" should "alter id in dataframe" in {
    val hashed = khll.hashIDCol(getNetflixRatings)
    assert(getNetflixRatings.select("field").except(hashed.select("field")).count() > 0)
    assert(getNetflixRatings.select("id").except(hashed.select("id")).count() == 0)
  }

  "hashFieldCol" should "alter value in dataframe" in {
    val hashed = khll.hashFieldCol(getNetflixRatings)
    assert(getNetflixRatings("field") != hashed("field"))
    assert(hashed.count() === k)
  }

  "khll" should "generates correctly" in {
    val fieldHashes = khll.hashFieldCol(getNetflixRatings)
    val idHashes = khll.hashIDCol(getNetflixRatings)

    val khllTable = khll.khll(fieldHashes, idHashes)
    println(khllTable.first())
  }
}
