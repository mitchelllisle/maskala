import org.mitchelllisle.reidentifiability.UniquenessAnalyser
import org.apache.spark.sql.DataFrame

class UniquenessAnalyserTest extends SparkFunSuite {
  import spark.implicits._

  val analyser = new UniquenessAnalyser

  def getNetflixRatings: DataFrame = {
    // Assuming sampleNetflixData is a DataFrame with the necessary structure
    analyser.createSourceTable(sampleNetflixData, Seq("rating"), "customerId")
  }

  "Getting table" should "return a non empty dataframe" in {
    assert(!getNetflixRatings.isEmpty)
  }

  // The following test might be modified or removed depending on the exact functionality of uniquenessData
  "Uniqueness data" should "match what is expected" in {
    val expected = Seq((3, 2427), (5, 2175), (1, 1108), (4, 2905), (2, 1243)).toDF("uniqueness", "count")
    val unique = analyser.calculateUniquenessData(getNetflixRatings)
    assert(unique.except(expected).count() == 0)
  }

  // The following test might be modified or removed depending on the exact functionality of uniquenessDistribution
  "Distribution" should "match expected values" in {
    val expected = Seq((1, 2427, 0.2), (1, 2175, 0.2), (1, 1108, 0.2), (1, 2905, 0.2), (1, 1243, 0.2)).toDF("uniqueness", "value_count", "value_ratio")

    val unique = analyser.calculateUniquenessData(getNetflixRatings)
    val numValues = analyser.countNumValues(unique)
    val distribution = analyser.calculateUniquenessDistribution(unique, numValues)

    assert(distribution.except(expected).count() == 0)
  }

  "Running pipeline" should "produce uniqueness results" in {
    val expected = Seq(
      (1, 2427, 0.2, 4, 0.8),
      (1, 2175, 0.2, 3, 0.6000000000000001),
      (1, 1108, 0.2, 1, 0.2),
      (1, 2905, 0.2, 5, 1.0),
      (1, 1243, 0.2, 2, 0.4)
    ).toDF("uniqueness", "value_count", "value_ratio", "cumulativeValueCount", "cumulativeValueRatio")
    val uniqueness = analyser.apply(getNetflixRatings, Seq("rating"), "customerId")
    assert(uniqueness.except(expected).count() == 0)
  }
}
