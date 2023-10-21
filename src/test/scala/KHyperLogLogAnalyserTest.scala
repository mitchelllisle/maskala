import org.apache.spark.sql.DataFrame
import org.mitchelllisle.khyperloglog.{KHyperLogLogAnalyser, KLLRow}

class KHyperLogLogAnalyserTest extends SparkFunSuite {
  val khll = new KHyperLogLogAnalyser(spark, k = 2056)
  val table: DataFrame = khll.getTable("netflix", "ratings", "customerId", Seq("rating"))

  val tableName = "netflix.ratings"

  test("test that getting table works") {
    assert(!table.isEmpty)
  }

  test("test hashIDCol hashes correctly") {
    val hashed = khll.hashIDCol(table)
    assert(table("field") != hashed("field"))
    assert(table("id") == hashed("id"))
  }
}