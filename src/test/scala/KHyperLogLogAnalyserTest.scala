import org.apache.spark.sql.DataFrame
import org.mitchelllisle.khyperloglog.KHyperLogLogAnalyser

class KHyperLogLogAnalyserTest extends SparkFunSuite {
  val k = 2056
  val khll = new KHyperLogLogAnalyser(spark, k = k)

  def getNetflixRatings: DataFrame = {
    khll.getTable("netflix", "ratings", "customerId", Seq("rating"))
  }

  val tableName = "netflix.ratings"

  "hashIDCol" should "alter id in dataframe" in {
    val hashed = khll.hashIDCol(getNetflixRatings)
    assert(getNetflixRatings("field") != hashed("field"))
    assert(getNetflixRatings("id") == hashed("id"))
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
