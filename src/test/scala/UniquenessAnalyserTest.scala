import org.mitchelllisle.reidentifiability.UniquenessAnalyser
import org.apache.spark.sql.DataFrame

class UniquenessAnalyserTest extends SparkFunSuite {
  import spark.implicits._

  val analyser: UniquenessAnalyser.type = UniquenessAnalyser

  "UniquenessAnalyser apply method" should "produce the correct cumulative distribution of uniqueness" in {
    // Group by 'item' and use 'userId' as the user ID column
    val result = UniquenessAnalyser(sampleNetflixData, Seq("rating", "movie"), "user_id")

    val columns = Seq("uniqueness", "value_count", "value_ratio", "cumulativeValueCount", "cumulativeValueRatio")
    assert(result.columns.sameElements(columns))
    assert(result.count() == 36)
  }
}
