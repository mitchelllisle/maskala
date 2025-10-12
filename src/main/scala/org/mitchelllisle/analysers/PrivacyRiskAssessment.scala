package org.mitchelllisle.analysers

import org.apache.spark.sql.{DataFrame, functions => F}

/** Represents the result of a privacy risk assessment.
  *
  * @param kAnonymityScore
  *   The minimum group size (k value) in the dataset. Higher is better.
  * @param lDiversityScore
  *   The minimum diversity (l value) in the dataset. Higher is better.
  * @param tClosenessScore
  *   The maximum distance from the overall distribution. Lower is better.
  * @param uniquenessRisk
  *   The ratio of records with uniqueness = 1 (highly identifiable). Lower is better.
  * @param overallRiskScore
  *   A normalized risk score from 0-100, where 0 is lowest risk and 100 is highest risk.
  * @param recommendations
  *   List of actionable recommendations to improve privacy.
  */
case class RiskAssessmentResult(
    kAnonymityScore: Long,
    lDiversityScore: Option[Long],
    tClosenessScore: Option[Double],
    uniquenessRisk: Double,
    overallRiskScore: Double,
    recommendations: List[String]
)

/** Parameters for quasi-identifier detection.
  *
  * @param quasiIdentifiers
  *   Columns that are quasi-identifiers (can be used to re-identify individuals).
  * @param sensitiveAttribute
  *   Optional sensitive attribute column for l-diversity and t-closeness analysis.
  * @param idColumn
  *   Optional column that uniquely identifies individuals.
  */
case class PrivacyRiskParams(
    quasiIdentifiers: Seq[String],
    sensitiveAttribute: Option[String] = None,
    idColumn: Option[String] = None
)

/** A module for comprehensive privacy risk assessment of Spark DataFrames.
  *
  * This module evaluates re-identification risks by calculating various privacy metrics including k-anonymity,
  * l-diversity, t-closeness, and uniqueness analysis. It provides risk scores and actionable recommendations for
  * further anonymization.
  */
object PrivacyRiskAssessment {

  /** Performs a comprehensive privacy risk assessment on the given DataFrame.
    *
    * @param data
    *   The DataFrame to assess.
    * @param params
    *   Privacy risk assessment parameters including quasi-identifiers and sensitive attributes.
    * @param kThreshold
    *   The desired k value for k-anonymity (default: 5).
    * @param lThreshold
    *   The desired l value for l-diversity (default: 3).
    * @param tThreshold
    *   The desired t value for t-closeness (default: 0.2).
    * @return
    *   A RiskAssessmentResult containing scores and recommendations.
    */
  def assess(
      data: DataFrame,
      params: PrivacyRiskParams,
      kThreshold: Int = 5,
      lThreshold: Int = 3,
      tThreshold: Double = 0.2
  ): RiskAssessmentResult = {

    // Validate inputs
    require(params.quasiIdentifiers.nonEmpty, "At least one quasi-identifier must be specified")
    require(kThreshold > 0, "k threshold must be positive")
    require(lThreshold > 0, "l threshold must be positive")
    require(tThreshold > 0 && tThreshold <= 1, "t threshold must be between 0 and 1")

    // Select only quasi-identifiers for analysis (exclude ID column if present)
    val quasiData = params.idColumn match {
      case Some(id) => data.select((params.quasiIdentifiers :+ id).map(F.col): _*)
      case None     => data.select(params.quasiIdentifiers.map(F.col): _*)
    }

    // Calculate k-anonymity
    val kAnon = new KAnonymity(kThreshold)
    val kAnonymityData = kAnon(quasiData, params.idColumn)
    val minK = kAnonymityData
      .agg(F.min("count").as("min_k"))
      .first()
      .getAs[Long]("min_k")

    // Calculate l-diversity if sensitive attribute is provided
    val lDiversityScore = params.sensitiveAttribute.map { sensCol =>
      val dataWithSens = data.select((params.quasiIdentifiers :+ sensCol).map(F.col): _*)
      val lDiv = new LDiversity(lThreshold, 1)
      val lDivData = lDiv(dataWithSens, sensCol)
      lDivData
        .agg(F.min("distinctCount").as("min_l"))
        .first()
        .getAs[Long]("min_l")
    }

    // Calculate t-closeness if sensitive attribute is provided
    val tClosenessScore = params.sensitiveAttribute.map { sensCol =>
      val dataWithSens = data.select((params.quasiIdentifiers :+ sensCol).map(F.col): _*)
      val tClose = new TCloseness(tThreshold, 1)
      val tCloseData = tClose(dataWithSens, sensCol)
      tCloseData
        .agg(F.max("distance").as("max_t"))
        .first()
        .getAs[Double]("max_t")
    }

    // Calculate uniqueness risk
    val uniquenessData = params.idColumn match {
      case Some(id) =>
        UniquenessAnalyser(data, params.quasiIdentifiers, id)
      case None =>
        // If no ID column, create a synthetic one
        val dataWithId = data.withColumn("_synthetic_id", F.monotonically_increasing_id())
        UniquenessAnalyser(dataWithId, params.quasiIdentifiers, "_synthetic_id")
    }

    val uniquenessRisk = uniquenessData
      .filter(F.col("uniqueness") === 1)
      .select(F.col("cumulativeValueRatio"))
      .collect()
      .headOption
      .map(_.getAs[Double](0))
      .getOrElse(0.0)

    // Calculate overall risk score (0-100, higher is worse)
    val kRisk = if (minK < kThreshold) {
      (1.0 - (minK.toDouble / kThreshold)) * 40.0 // Up to 40 points
    } else 0.0

    val lRisk = lDiversityScore match {
      case Some(l) if l < lThreshold =>
        (1.0 - (l.toDouble / lThreshold)) * 25.0 // Up to 25 points
      case _ => 0.0
    }

    val tRisk = tClosenessScore match {
      case Some(t) if t > tThreshold =>
        ((t - tThreshold) / (1.0 - tThreshold)) * 20.0 // Up to 20 points
      case _ => 0.0
    }

    val uRisk = uniquenessRisk * 15.0 // Up to 15 points

    val overallRisk = kRisk + lRisk + tRisk + uRisk

    // Generate recommendations
    val recommendations = generateRecommendations(
      minK,
      lDiversityScore,
      tClosenessScore,
      uniquenessRisk,
      kThreshold,
      lThreshold,
      tThreshold,
      params
    )

    RiskAssessmentResult(
      kAnonymityScore = minK,
      lDiversityScore = lDiversityScore,
      tClosenessScore = tClosenessScore,
      uniquenessRisk = uniquenessRisk,
      overallRiskScore = overallRisk,
      recommendations = recommendations
    )
  }

  /** Automatically detects potential quasi-identifiers in a DataFrame.
    *
    * This is a heuristic approach that identifies columns that might be quasi-identifiers based on their names and
    * characteristics. Columns with low cardinality relative to the dataset size are potential quasi-identifiers.
    *
    * @param data
    *   The DataFrame to analyze.
    * @param excludeColumns
    *   Columns to exclude from quasi-identifier detection (e.g., ID columns, sensitive attributes).
    * @param cardinalityThreshold
    *   The maximum ratio of distinct values to total rows for a column to be considered a quasi-identifier (default:
    *   0.5).
    * @return
    *   A sequence of column names that are likely quasi-identifiers.
    */
  def detectQuasiIdentifiers(
      data: DataFrame,
      excludeColumns: Seq[String] = Seq.empty,
      cardinalityThreshold: Double = 0.5
  ): Seq[String] = {
    val totalRows = data.count()
    val columns = data.columns.filterNot(excludeColumns.contains)

    // Common patterns for quasi-identifiers
    val quasiPatterns = Seq(
      "age",
      "gender",
      "zip",
      "zipcode",
      "postal",
      "city",
      "state",
      "country",
      "location",
      "birth",
      "education",
      "occupation",
      "race",
      "ethnicity"
    )

    columns.filter { colName =>
      val lowerName = colName.toLowerCase

      // Check if column name matches common patterns
      val matchesPattern = quasiPatterns.exists(pattern => lowerName.contains(pattern))

      // Check cardinality
      val distinctCount = data.select(colName).distinct().count()
      val cardinality = distinctCount.toDouble / totalRows
      val hasLowCardinality = cardinality <= cardinalityThreshold && distinctCount > 1

      matchesPattern || hasLowCardinality
    }.toSeq
  }

  private def generateRecommendations(
      minK: Long,
      lDiversity: Option[Long],
      tCloseness: Option[Double],
      uniquenessRisk: Double,
      kThreshold: Int,
      lThreshold: Int,
      tThreshold: Double,
      params: PrivacyRiskParams
  ): List[String] = {
    var recommendations = List.empty[String]

    // K-anonymity recommendations
    if (minK < kThreshold) {
      recommendations = recommendations :+ s"k-anonymity: Minimum group size is $minK, below threshold of $kThreshold. Consider generalizing quasi-identifiers (${params.quasiIdentifiers.mkString(", ")}) or removing outlier records."
    } else {
      recommendations = recommendations :+ s"k-anonymity: PASSED - Minimum group size is $minK (threshold: $kThreshold)."
    }

    // L-diversity recommendations
    lDiversity.foreach { l =>
      if (l < lThreshold) {
        recommendations = recommendations :+ s"l-diversity: Minimum diversity is $l, below threshold of $lThreshold. Consider adding more diverse values for the sensitive attribute or suppressing homogeneous groups."
      } else {
        recommendations = recommendations :+ s"l-diversity: PASSED - Minimum diversity is $l (threshold: $lThreshold)."
      }
    }

    // T-closeness recommendations
    tCloseness.foreach { t =>
      if (t > tThreshold) {
        recommendations = recommendations :+ f"t-closeness: Maximum distribution distance is $t%.3f, exceeds threshold of $tThreshold. The distribution of sensitive values in some groups differs significantly from the overall distribution. Consider redistributing values or removing non-representative groups."
      } else {
        recommendations = recommendations :+ f"t-closeness: PASSED - Maximum distribution distance is $t%.3f (threshold: $tThreshold)."
      }
    }

    // Uniqueness recommendations
    if (uniquenessRisk > 0.1) {
      recommendations = recommendations :+ f"Uniqueness: ${uniquenessRisk * 100}%.1f%% of records are highly unique (uniqueness=1), indicating high re-identification risk. Consider additional generalization or suppression."
    } else if (uniquenessRisk > 0.0) {
      recommendations = recommendations :+ f"Uniqueness: ${uniquenessRisk * 100}%.1f%% of records have uniqueness=1. Risk is moderate but acceptable."
    } else {
      recommendations = recommendations :+ "Uniqueness: PASSED - No highly unique records detected."
    }

    // General recommendations
    if (recommendations.exists(_.contains("below threshold")) || recommendations
          .exists(_.contains("exceeds threshold"))) {
      recommendations = recommendations :+ "General: Review the Anonymiser class to apply generalization strategies (RangeStrategy, DateStrategy, MappingStrategy) to quasi-identifiers."
    }

    recommendations
  }

  /** Generates a detailed privacy report as a formatted string.
    *
    * @param result
    *   The risk assessment result to report.
    * @return
    *   A formatted string containing the privacy report.
    */
  def generateReport(result: RiskAssessmentResult): String = {
    val sb = new StringBuilder

    sb.append("=" * 80)
    sb.append("\n")
    sb.append("PRIVACY RISK ASSESSMENT REPORT\n")
    sb.append("=" * 80)
    sb.append("\n\n")

    sb.append(s"Overall Risk Score: ${result.overallRiskScore.toInt}/100\n")
    sb.append(getRiskLevel(result.overallRiskScore))
    sb.append("\n\n")

    sb.append("-" * 80)
    sb.append("\n")
    sb.append("PRIVACY METRICS\n")
    sb.append("-" * 80)
    sb.append("\n")
    sb.append(f"k-Anonymity Score: ${result.kAnonymityScore}%d\n")
    result.lDiversityScore.foreach(l => sb.append(f"l-Diversity Score: $l%d\n"))
    result.tClosenessScore.foreach(t => sb.append(f"t-Closeness Score: $t%.3f\n"))
    sb.append(f"Uniqueness Risk: ${result.uniquenessRisk * 100}%.2f%%\n")

    sb.append("\n")
    sb.append("-" * 80)
    sb.append("\n")
    sb.append("RECOMMENDATIONS\n")
    sb.append("-" * 80)
    sb.append("\n")

    result.recommendations.zipWithIndex.foreach { case (rec, idx) =>
      sb.append(s"${idx + 1}. $rec\n")
    }

    sb.append("\n")
    sb.append("=" * 80)
    sb.append("\n")

    sb.toString()
  }

  private def getRiskLevel(score: Double): String = {
    if (score < 20) "Risk Level: LOW ✓"
    else if (score < 40) "Risk Level: MODERATE ⚠"
    else if (score < 60) "Risk Level: HIGH ⚠⚠"
    else "Risk Level: CRITICAL ⚠⚠⚠"
  }
}
