import org.mitchelllisle.khyperloglog.UniquenessAnalyser
import org.apache.spark.sql.DataFrame
import org.scalatest.BeforeAndAfterAll


class UniquenessAnalyserTest extends SparkFunSuite with BeforeAndAfterAll {
  val k = 2056
  val analyser = new UniquenessAnalyser(spark)
  val tableName = "netflix.ratings"

  import spark.implicits._

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
}
