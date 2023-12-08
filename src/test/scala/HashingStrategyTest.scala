import org.mitchelllisle.redaction.HashingStrategy

import org.apache.spark.sql.DataFrame

class HashingStrategyTest extends SparkFunSuite {

  def columnToSeq(data: DataFrame, column: String): Array[String] = {
    data.select(column).rdd.map(row => row.getAs[String](column)).collect()
  }

  import spark.implicits._

  "HashingStrategy" should "correctly hash values based on column name" in {
    val strategy = HashingStrategy("user_id")

    val redactedData = strategy(sampleNetflixData)
    val results = redactedData.collect().map(_.getString(0))

    val rawUserIds = sampleNetflixData.collect().map(_.getString(0))

    assert(results.forall(_.length == 64))
    assert(!results.sameElements(rawUserIds))
  }

  "Hashing with salt and pepper" should "hash differently than without it" in {
    val column = "user_id"
    val strategy = HashingStrategy(column)

    val redactedOutput = columnToSeq(strategy(sampleNetflixData), column)
    val redactedDataWithSaltAndPepper = columnToSeq(strategy(sampleNetflixData, "my-salt", "my-pepper"), column)
    val redactedDataWithSalt = columnToSeq(strategy(sampleNetflixData, salt="my-salt"), column)
    val redactedDataWithPepper = columnToSeq(strategy(sampleNetflixData,  pepper="my-pepper"), column)

    redactedOutput.indices.foreach(i => {
      val hashedValues = Seq(
        redactedOutput(i), redactedDataWithSaltAndPepper(i), redactedDataWithSalt(i), redactedDataWithPepper(i)
      )
      assert(hashedValues.distinct.size == hashedValues.size)
    })
  }
}
