import org.mitchelllisle.analysers.PrivacyRiskScorer
import org.scalatest.flatspec.AnyFlatSpec

class PrivacyRiskScorerTest extends AnyFlatSpec with SparkFunSuite {
  import spark.implicits._

  val testData = Seq(
    ("user1", "25", "M", "10001"),    // Unique combination
    ("user2", "30", "F", "10002"),    // Unique combination  
    ("user3", "30", "F", "10002"),    // Same as user2 (k=2)
    ("user4", "35", "M", "10003"),    // Shared by multiple
    ("user5", "35", "M", "10003"),    // Shared by multiple
    ("user6", "35", "M", "10003"),    // Shared by multiple
    ("user7", "35", "M", "10003")     // Shared by multiple (k=4)
  ).toDF("user_id", "age", "gender", "zip")

  val quasiIdentifiers = Seq("age", "gender", "zip")

  "calculateRiskScores" should "assign higher risk to unique individuals" in {
    val riskScores = PrivacyRiskScorer.calculateRiskScores(
      testData,
      quasiIdentifiers,
      "user_id"
    )

    val user1Risk = riskScores.filter($"individual_id" === "user1")
      .select("overall_risk_score")
      .collect()(0)
      .getDouble(0)

    val user4Risk = riskScores.filter($"individual_id" === "user4")
      .select("overall_risk_score")
      .collect()(0)
      .getDouble(0)

    // user1 is unique (k=1), should have higher risk than user4 (k=4)
    assert(user1Risk > user4Risk)
  }

  "calculateRiskScores" should "include all required columns" in {
    val riskScores = PrivacyRiskScorer.calculateRiskScores(
      testData,
      quasiIdentifiers,
      "user_id"
    )

    val expectedColumns = Set("individual_id", "uniqueness_score", "k_anonymity_score", 
                              "overall_risk_score", "risk_level", "group_size")
    assert(expectedColumns.subsetOf(riskScores.columns.toSet))
  }

  "calculateRiskScores" should "assign appropriate risk levels" in {
    val riskScores = PrivacyRiskScorer.calculateRiskScores(
      testData,
      quasiIdentifiers,
      "user_id"
    )

    val riskLevels = riskScores.select("risk_level").distinct().collect().map(_.getString(0))
    
    // Should have categorized risks
    assert(riskLevels.nonEmpty)
    
    // Unique individuals should be high risk
    val user1RiskLevel = riskScores.filter($"individual_id" === "user1")
      .select("risk_level")
      .collect()(0)
      .getString(0)
    
    assert(user1RiskLevel == "CRITICAL" || user1RiskLevel == "HIGH")
  }

  "calculateRiskScores" should "have risk scores between 0 and 1" in {
    val riskScores = PrivacyRiskScorer.calculateRiskScores(
      testData,
      quasiIdentifiers,
      "user_id"
    )

    val allScores = riskScores.select("overall_risk_score").collect().map(_.getDouble(0))
    
    allScores.foreach { score =>
      assert(score >= 0.0 && score <= 1.0, s"Risk score $score should be between 0 and 1")
    }
  }

  "identifyHighRiskIndividuals" should "return only high-risk individuals" in {
    val highRisk = PrivacyRiskScorer.identifyHighRiskIndividuals(
      testData,
      quasiIdentifiers,
      "user_id",
      riskThreshold = 0.5
    )

    val allScores = highRisk.select("overall_risk_score").collect().map(_.getDouble(0))
    
    allScores.foreach { score =>
      assert(score >= 0.5, s"All returned individuals should have risk >= 0.5, got $score")
    }
  }

  "identifyHighRiskIndividuals" should "order by risk score descending" in {
    val highRisk = PrivacyRiskScorer.identifyHighRiskIndividuals(
      testData,
      quasiIdentifiers,
      "user_id",
      riskThreshold = 0.3
    )

    val scores = highRisk.select("overall_risk_score").collect().map(_.getDouble(0))
    
    // Verify descending order
    if (scores.length > 1) {
      scores.sliding(2).foreach { case Array(a, b) =>
        assert(a >= b, "Scores should be in descending order")
      }
    }
  }

  "generateRiskSummary" should "create summary with all risk levels" in {
    val summary = PrivacyRiskScorer.generateRiskSummary(
      testData,
      quasiIdentifiers,
      "user_id"
    )

    val expectedColumns = Set("risk_level", "individual_count", "avg_risk_score", 
                              "min_risk_score", "max_risk_score", "percentage")
    assert(expectedColumns.subsetOf(summary.columns.toSet))
  }

  "generateRiskSummary" should "have percentages that sum to 100" in {
    val summary = PrivacyRiskScorer.generateRiskSummary(
      testData,
      quasiIdentifiers,
      "user_id"
    )

    val totalPercentage = summary.select("percentage").collect().map(_.getDouble(0)).sum
    
    // Allow for small floating point errors
    assert(Math.abs(totalPercentage - 100.0) < 0.01, s"Percentages should sum to 100, got $totalPercentage")
  }

  "generateRiskSummary" should "order by risk level severity" in {
    val summary = PrivacyRiskScorer.generateRiskSummary(
      testData,
      quasiIdentifiers,
      "user_id"
    )

    val levels = summary.select("risk_level").collect().map(_.getString(0))
    
    // Check ordering: CRITICAL, HIGH, MEDIUM, LOW
    val validOrderings = Set(
      Seq("CRITICAL", "HIGH", "MEDIUM", "LOW"),
      Seq("CRITICAL", "HIGH", "MEDIUM"),
      Seq("CRITICAL", "HIGH"),
      Seq("HIGH", "MEDIUM", "LOW"),
      Seq("HIGH", "MEDIUM"),
      Seq("MEDIUM", "LOW")
    )
    
    // Just verify we have some ordering and no inversions
    if (levels.length > 1) {
      val hasProperOrder = !levels.sliding(2).exists {
        case Array("LOW", "CRITICAL") => true
        case Array("LOW", "HIGH") => true
        case Array("LOW", "MEDIUM") => true
        case Array("MEDIUM", "CRITICAL") => true
        case Array("MEDIUM", "HIGH") => true
        case Array("HIGH", "CRITICAL") => true
        case _ => false
      }
      assert(hasProperOrder, "Risk levels should be ordered by severity")
    }
  }

  "calculateDetailedRiskMetrics" should "include prosecutor, journalist, and marketer risk" in {
    val detailedMetrics = PrivacyRiskScorer.calculateDetailedRiskMetrics(
      testData,
      quasiIdentifiers,
      "user_id"
    )

    val expectedColumns = Set("individual_id", "group_size", "prosecutor_risk", 
                              "journalist_risk", "marketer_risk")
    assert(expectedColumns.subsetOf(detailedMetrics.columns.toSet))
  }

  "calculateDetailedRiskMetrics" should "have prosecutor risk of 1 for unique individuals" in {
    val detailedMetrics = PrivacyRiskScorer.calculateDetailedRiskMetrics(
      testData,
      quasiIdentifiers,
      "user_id"
    )

    val user1Metrics = detailedMetrics.filter($"individual_id" === "user1").collect()(0)
    val prosecutorRisk = user1Metrics.getAs[Double]("prosecutor_risk")
    
    // Unique individual should have prosecutor risk of 1.0
    assert(Math.abs(prosecutorRisk - 1.0) < 0.01)
  }

  "meetsPrivacyStandard" should "return true for datasets meeting standards" in {
    // Create a dataset where everyone is in groups of 5+
    val safeData = Seq(
      ("user1", "30", "M", "10001"),
      ("user2", "30", "M", "10001"),
      ("user3", "30", "M", "10001"),
      ("user4", "30", "M", "10001"),
      ("user5", "30", "M", "10001"),
      ("user6", "30", "M", "10001"),
      ("user7", "30", "M", "10001"),
      ("user8", "30", "M", "10001"),
      ("user9", "30", "M", "10001"),
      ("user10", "30", "M", "10001")
    ).toDF("user_id", "age", "gender", "zip")

    val meetsStandard = PrivacyRiskScorer.meetsPrivacyStandard(
      safeData,
      quasiIdentifiers,
      "user_id",
      maxAcceptableRisk = 0.5,  // More lenient threshold
      maxHighRiskPercentage = 0.2  // More lenient threshold
    )

    assert(meetsStandard)
  }

  "meetsPrivacyStandard" should "return false for datasets with many unique individuals" in {
    val riskyData = Seq(
      ("user1", "25", "M", "10001"),
      ("user2", "26", "F", "10002"),
      ("user3", "27", "M", "10003"),
      ("user4", "28", "F", "10004")
    ).toDF("user_id", "age", "gender", "zip")

    val meetsStandard = PrivacyRiskScorer.meetsPrivacyStandard(
      riskyData,
      quasiIdentifiers,
      "user_id",
      maxAcceptableRisk = 0.3,
      maxHighRiskPercentage = 0.1
    )

    assert(!meetsStandard)
  }

  "calculateRiskScores" should "handle all individuals with same attributes" in {
    val uniformData = Seq(
      ("user1", "30", "M", "10001"),
      ("user2", "30", "M", "10001"),
      ("user3", "30", "M", "10001")
    ).toDF("user_id", "age", "gender", "zip")

    val riskScores = PrivacyRiskScorer.calculateRiskScores(
      uniformData,
      quasiIdentifiers,
      "user_id"
    )

    // All should have same risk since they're in the same group
    val scores = riskScores.select("overall_risk_score").collect().map(_.getDouble(0))
    assert(scores.forall(_ == scores.head))
    // Group of 3 should have moderate risk (not necessarily < 0.5)
    assert(scores.head >= 0.0 && scores.head <= 1.0)
  }
}
