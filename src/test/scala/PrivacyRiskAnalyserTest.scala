import org.apache.spark.sql.DataFrame
import org.mitchelllisle.analysers.{PrivacyRiskAnalyser, PrivacyRiskParams}
import org.scalatest.flatspec.AnyFlatSpec

class PrivacyRiskAnalyserTest extends AnyFlatSpec with SparkFunSuite {
  import spark.implicits._

  "PrivacyRiskAnalyser" should "assess privacy risk for k-anonymous data" in {
    val data: DataFrame = Seq(
      ("user1", "30", "Male", "Engineer"),
      ("user2", "30", "Male", "Engineer"),
      ("user3", "30", "Male", "Doctor"),
      ("user4", "25", "Female", "Teacher"),
      ("user5", "25", "Female", "Teacher")
    ).toDF("user_id", "age", "gender", "occupation")

    val assessment = PrivacyRiskAnalyser(
      data = data,
      k = 2,
      userIdColumn = "user_id",
      groupByColumns = Seq("age", "gender")
    )

    // The data should be k-anonymous (k=2)
    assert(assessment.kAnonymityScore > 0.8)
    assert(assessment.overallRiskLevel == "LOW" || assessment.overallRiskLevel == "MEDIUM")
    assert(assessment.recommendations.nonEmpty)
    assert(assessment.metrics.contains("kAnonymity"))
    assert(assessment.metrics.contains("uniqueness"))
  }

  "PrivacyRiskAnalyser" should "detect non-k-anonymous data" in {
    val data: DataFrame = Seq(
      ("user1", "30", "Male"),
      ("user2", "30", "Male"),
      ("user3", "25", "Female"),
      ("user4", "35", "Other")
    ).toDF("user_id", "age", "gender")

    val assessment = PrivacyRiskAnalyser(
      data = data,
      k = 2,
      userIdColumn = "user_id",
      groupByColumns = Seq("age", "gender")
    )

    // The data has unique records, so k-anonymity score should be lower
    assert(assessment.kAnonymityScore < 1.0)
    assert(assessment.recommendations.exists(_.contains("does not satisfy")))
  }

  "PrivacyRiskAnalyser" should "assess privacy risk with l-diversity" in {
    val data: DataFrame = Seq(
      ("user1", "A", "Disease1"),
      ("user2", "A", "Disease2"),
      ("user3", "A", "Disease3"),
      ("user4", "B", "Disease1"),
      ("user5", "B", "Disease2")
    ).toDF("user_id", "group", "diagnosis")

    val assessment = PrivacyRiskAnalyser(
      data = data,
      k = 2,
      l = 2,
      userIdColumn = "user_id",
      sensitiveColumn = "diagnosis",
      groupByColumns = Seq("group")
    )

    // Should include both k-anonymity and l-diversity scores
    assert(assessment.kAnonymityScore >= 0.0)
    assert(assessment.lDiversityScore.isDefined)
    assert(assessment.lDiversityScore.get >= 0.0)
    assert(assessment.metrics.contains("kAnonymity"))
    assert(assessment.metrics.contains("lDiversity"))
    assert(assessment.metrics.contains("uniqueness"))
  }

  "PrivacyRiskAnalyser" should "detect lack of l-diversity" in {
    val data: DataFrame = Seq(
      ("user1", "A", "Disease1"),
      ("user2", "A", "Disease1"),
      ("user3", "B", "Disease2"),
      ("user4", "B", "Disease2")
    ).toDF("user_id", "group", "diagnosis")

    val assessment = PrivacyRiskAnalyser(
      data = data,
      k = 2,
      l = 2,
      userIdColumn = "user_id",
      sensitiveColumn = "diagnosis",
      groupByColumns = Seq("group")
    )

    // L-diversity should fail (only 1 distinct value per group)
    assert(assessment.lDiversityScore.isDefined)
    assert(assessment.lDiversityScore.get < 0.5)
    assert(assessment.recommendations.exists(_.contains("diversity")))
  }

  "PrivacyRiskAnalyser" should "provide risk level categorization" in {
    val highRiskData: DataFrame = Seq(
      ("user1", "30", "Male"),
      ("user2", "25", "Female"),
      ("user3", "35", "Other"),
      ("user4", "40", "Male")
    ).toDF("user_id", "age", "gender")

    val assessment = PrivacyRiskAnalyser(
      data = highRiskData,
      k = 2,
      userIdColumn = "user_id",
      groupByColumns = Seq("age", "gender")
    )

    // Should categorize as high or critical risk
    assert(
      assessment.overallRiskLevel == "HIGH" ||
        assessment.overallRiskLevel == "CRITICAL" ||
        assessment.overallRiskLevel == "MEDIUM"
    )
    assert(assessment.overallRiskScore >= 0.0 && assessment.overallRiskScore <= 1.0)
  }

  "PrivacyRiskAnalyser" should "use PrivacyRiskParams case class" in {
    val data: DataFrame = Seq(
      ("user1", "30", "Male"),
      ("user2", "30", "Male"),
      ("user3", "25", "Female"),
      ("user4", "25", "Female")
    ).toDF("user_id", "age", "gender")

    val params = PrivacyRiskParams(
      k = 2,
      l = None,
      userIdColumn = "user_id",
      sensitiveColumn = None,
      groupByColumns = Seq("age", "gender")
    )

    val assessment = PrivacyRiskAnalyser(data, params)

    assert(assessment.kAnonymityScore > 0.8)
    assert(assessment.lDiversityScore.isEmpty)
    assert(assessment.recommendations.nonEmpty)
  }

  "PrivacyRiskAnalyser" should "generate appropriate recommendations" in {
    val data: DataFrame = Seq(
      ("user1", "30", "Male"),
      ("user2", "30", "Male")
    ).toDF("user_id", "age", "gender")

    val assessment = PrivacyRiskAnalyser(
      data = data,
      k = 2,
      userIdColumn = "user_id",
      groupByColumns = Seq("age", "gender")
    )

    // Should have recommendations
    assert(assessment.recommendations.nonEmpty)
    assert(assessment.recommendations.size >= 1)
  }
}
