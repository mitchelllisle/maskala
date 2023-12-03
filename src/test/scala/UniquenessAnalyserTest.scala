import org.mitchelllisle.reidentifiability.{KHyperLogLogAnalyser, UniquenessAnalyser}
import org.apache.spark.sql.DataFrame


class UniquenessAnalyserTest extends SparkFunSuite {
  val k = 2056
  val khll = new KHyperLogLogAnalyser(spark)
  val analyser = new UniquenessAnalyser(spark)

  import spark.implicits._

  val sampleNetflixData: DataFrame = spark
    .read
    .option("header", "true")
    .csv("src/test/resources/netflix-sample.csv")


  def getNetflixRatings: DataFrame = {
    analyser.hashData(sampleNetflixData, "customerId", Seq("rating"))
  }

  "Getting table" should "return a non empty dataframe" in {
    assert(!getNetflixRatings.isEmpty)
  }

  "Uniqueness data" should "match what is expected" in {
    val expected = Seq((3, 2427), (5, 2175), (1, 1108), (4, 2905), (2, 1243)).toDF()
    val unique = analyser.uniquenessData(getNetflixRatings)
    assert(unique.except(expected).count() == 0)
  }

  "Distribution" should "match expected values" in {
    val expected = Seq((2427, 1, 0.2), (2175, 1, 0.2), (1108, 1, 0.2), (2905, 1, 0.2), (1243, 1, 0.2)).toDF()

    val unique = analyser.uniquenessData(getNetflixRatings)

    val distribution = analyser.uniquenessDistribution(unique, unique.count())

    assert(distribution.except(expected).count() == 0)
  }

  "Running pipeline" should "produce uniqueness results" in {
    val expected = Seq(
      (2427, 1, 0.2, 4 ,0.8),
      (2175, 1, 0.2, 3, 0.6000000000000001),
      (1108, 1, 0.2, 1, 0.2),
      (2905, 1, 0.2, 5, 1.0),
      (1243, 1, 0.2, 2, 0.4)
    ).toDF()
    val uniqueness = analyser(getNetflixRatings)
    assert(uniqueness.except(expected).count() == 0)
  }

  "getTable" should "prepare the data with hash values" in {
    val prepared = khll.hashData(sampleNetflixData, Seq("rating"))

    assert(prepared.columns.contains("field"))

    assert(prepared.count() > 0L)
    assert(prepared.except(sampleNetflixData).count() > 0L)
  }

  "khllCardinality" should "estimate cardinality with limited hash values" in {
    val prepared = khll.hashData(sampleNetflixData, Seq("field", "id"))
    val khllEstimate = khll.khllCardinality(prepared, k)

    assert(khllEstimate.columns.contains("approx_count_distinct(hash)"))
    assert(khllEstimate.count() == 1)
  }

  "apply" should "execute the complete KHLL analysis workflow" in {
    val result = khll.apply(sampleNetflixData, Seq("field", "id"), k)

    assert(result.columns.contains("approx_count_distinct(hash)"))
    assert(result.count() == 1)
  }
}
