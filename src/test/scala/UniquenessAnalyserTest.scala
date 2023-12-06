import org.mitchelllisle.reidentifiability.UniquenessAnalyser


class UniquenessAnalyserTest extends SparkFunSuite {
  import spark.implicits._

  val analyser = new UniquenessAnalyser(spark)

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
}
