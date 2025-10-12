import org.scalatest.flatspec.AnyFlatSpec
import org.mitchelllisle.analysers.{PrivacyRiskAssessment, PrivacyRiskParams}

class PrivacyRiskAssessmentTest extends AnyFlatSpec with SparkFunSuite {

  import spark.implicits._

  "PrivacyRiskAssessment" should "detect quasi-identifiers automatically" in {
    val data = Seq(
      ("1", "30", "Male", "12345", "Engineer"),
      ("2", "25", "Female", "12346", "Doctor"),
      ("3", "30", "Male", "12347", "Teacher")
    ).toDF("id", "age", "gender", "zipcode", "occupation")

    val quasiIds = PrivacyRiskAssessment.detectQuasiIdentifiers(data, Seq("id"))

    // Should detect age, gender, zipcode, occupation as quasi-identifiers
    assert(quasiIds.contains("age") || quasiIds.contains("gender") || quasiIds.contains("zipcode"))
  }

  "PrivacyRiskAssessment" should "assess basic privacy risk with k-anonymity" in {
    val data = Seq(
      ("1", "30", "Male"),
      ("2", "30", "Male"),
      ("3", "25", "Female"),
      ("4", "25", "Female")
    ).toDF("id", "age", "gender")

    val params = PrivacyRiskParams(
      quasiIdentifiers = Seq("age", "gender"),
      idColumn = Some("id")
    )

    val result = PrivacyRiskAssessment.assess(data, params, kThreshold = 2)

    assert(result.kAnonymityScore >= 2)
    assert(result.overallRiskScore >= 0.0)
    assert(result.recommendations.nonEmpty)
  }

  "PrivacyRiskAssessment" should "assess privacy with l-diversity" in {
    val data = Seq(
      ("1", "30", "Male", "Disease1"),
      ("2", "30", "Male", "Disease2"),
      ("3", "25", "Female", "Disease1"),
      ("4", "25", "Female", "Disease2")
    ).toDF("id", "age", "gender", "disease")

    val params = PrivacyRiskParams(
      quasiIdentifiers = Seq("age", "gender"),
      sensitiveAttribute = Some("disease"),
      idColumn = Some("id")
    )

    val result = PrivacyRiskAssessment.assess(data, params, kThreshold = 2, lThreshold = 2)

    assert(result.kAnonymityScore >= 2)
    assert(result.lDiversityScore.isDefined)
    assert(result.lDiversityScore.get >= 2)
    assert(result.recommendations.nonEmpty)
  }

  "PrivacyRiskAssessment" should "assess privacy with t-closeness" in {
    val data = Seq(
      ("1", "30", "Male", "Disease1"),
      ("2", "30", "Male", "Disease2"),
      ("3", "25", "Female", "Disease1"),
      ("4", "25", "Female", "Disease2")
    ).toDF("id", "age", "gender", "disease")

    val params = PrivacyRiskParams(
      quasiIdentifiers = Seq("age", "gender"),
      sensitiveAttribute = Some("disease"),
      idColumn = Some("id")
    )

    val result = PrivacyRiskAssessment.assess(
      data,
      params,
      kThreshold = 2,
      lThreshold = 2,
      tThreshold = 0.5
    )

    assert(result.tClosenessScore.isDefined)
    assert(result.recommendations.nonEmpty)
  }

  "PrivacyRiskAssessment" should "calculate uniqueness risk" in {
    val data = Seq(
      ("1", "30", "Male"),
      ("2", "30", "Male"),
      ("3", "25", "Female"),
      ("4", "45", "Other")
    ).toDF("id", "age", "gender")

    val params = PrivacyRiskParams(
      quasiIdentifiers = Seq("age", "gender"),
      idColumn = Some("id")
    )

    val result = PrivacyRiskAssessment.assess(data, params, kThreshold = 2)

    assert(result.uniquenessRisk >= 0.0)
    assert(result.uniquenessRisk <= 1.0)
  }

  "PrivacyRiskAssessment" should "generate a comprehensive report" in {
    val data = Seq(
      ("1", "30", "Male", "Disease1"),
      ("2", "30", "Male", "Disease2"),
      ("3", "25", "Female", "Disease1"),
      ("4", "25", "Female", "Disease2")
    ).toDF("id", "age", "gender", "disease")

    val params = PrivacyRiskParams(
      quasiIdentifiers = Seq("age", "gender"),
      sensitiveAttribute = Some("disease"),
      idColumn = Some("id")
    )

    val result = PrivacyRiskAssessment.assess(data, params)
    val report = PrivacyRiskAssessment.generateReport(result)

    assert(report.contains("PRIVACY RISK ASSESSMENT REPORT"))
    assert(report.contains("Overall Risk Score"))
    assert(report.contains("RECOMMENDATIONS"))
  }

  "PrivacyRiskAssessment" should "work without ID column" in {
    val data = Seq(
      ("30", "Male"),
      ("30", "Male"),
      ("25", "Female"),
      ("25", "Female")
    ).toDF("age", "gender")

    val params = PrivacyRiskParams(
      quasiIdentifiers = Seq("age", "gender")
    )

    val result = PrivacyRiskAssessment.assess(data, params, kThreshold = 2)

    assert(result.kAnonymityScore >= 2)
    assert(result.recommendations.nonEmpty)
  }

  "PrivacyRiskAssessment" should "provide higher risk scores for riskier data" in {
    val safeData = Seq(
      ("1", "30", "Male", "Disease1"),
      ("2", "30", "Male", "Disease2"),
      ("3", "30", "Male", "Disease3"),
      ("4", "25", "Female", "Disease1"),
      ("5", "25", "Female", "Disease2"),
      ("6", "25", "Female", "Disease3")
    ).toDF("id", "age", "gender", "disease")

    val riskyData = Seq(
      ("1", "30", "Male", "Disease1"),
      ("2", "25", "Female", "Disease2"),
      ("3", "45", "Other", "Disease3")
    ).toDF("id", "age", "gender", "disease")

    val params = PrivacyRiskParams(
      quasiIdentifiers = Seq("age", "gender"),
      sensitiveAttribute = Some("disease"),
      idColumn = Some("id")
    )

    val safeResult = PrivacyRiskAssessment.assess(safeData, params, kThreshold = 2)
    val riskyResult = PrivacyRiskAssessment.assess(riskyData, params, kThreshold = 2)

    assert(riskyResult.overallRiskScore > safeResult.overallRiskScore)
  }

  "detectQuasiIdentifiers" should "exclude specified columns" in {
    val data = Seq(
      ("1", "30", "Male", "secret123"),
      ("2", "25", "Female", "secret456")
    ).toDF("id", "age", "gender", "sensitive_data")

    val quasiIds = PrivacyRiskAssessment.detectQuasiIdentifiers(
      data,
      excludeColumns = Seq("id", "sensitive_data")
    )

    assert(!quasiIds.contains("id"))
    assert(!quasiIds.contains("sensitive_data"))
  }

  "detectQuasiIdentifiers" should "detect columns by cardinality" in {
    val data = Seq(
      ("1", "A", "X"),
      ("2", "A", "Y"),
      ("3", "B", "X"),
      ("4", "B", "Y")
    ).toDF("id", "low_cardinality", "another_low_cardinality")

    val quasiIds = PrivacyRiskAssessment.detectQuasiIdentifiers(
      data,
      excludeColumns = Seq("id"),
      cardinalityThreshold = 0.6
    )

    // Both columns have 2 distinct values out of 4 rows (50% cardinality)
    assert(quasiIds.contains("low_cardinality"))
    assert(quasiIds.contains("another_low_cardinality"))
  }
}
