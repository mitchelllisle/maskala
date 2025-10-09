package org.mitchelllisle.analysers

import org.apache.spark.sql.{DataFrame, functions => F}

/** Represents the result of a comprehensive privacy risk analysis.
  *
  * @param kAnonymityScore Score for k-anonymity (0.0 to 1.0, where 1.0 is best)
  * @param lDiversityScore Score for l-diversity (0.0 to 1.0, where 1.0 is best)
  * @param uniquenessScore Score based on uniqueness analysis (0.0 to 1.0, where 1.0 is best)
  * @param overallRiskLevel Overall risk level: "LOW", "MEDIUM", "HIGH", "CRITICAL"
  * @param overallRiskScore Overall risk score (0.0 to 1.0, where 1.0 is safest)
  * @param recommendations List of recommendations to improve privacy
  * @param metrics Detailed metrics from the analysis
  */
case class PrivacyRiskAssessment(
    kAnonymityScore: Double,
    lDiversityScore: Option[Double],
    uniquenessScore: Double,
    overallRiskLevel: String,
    overallRiskScore: Double,
    recommendations: Seq[String],
    metrics: Map[String, Any]
)

case class PrivacyRiskParams(
    k: Int,
    l: Option[Int],
    userIdColumn: String,
    sensitiveColumn: Option[String],
    groupByColumns: Seq[String]
) extends AnalyserParams

/** Comprehensive privacy risk analyzer that evaluates re-identification risk using multiple privacy metrics.
  *
  * This analyzer combines K-Anonymity, L-Diversity (optional), and Uniqueness Analysis to provide a holistic view of
  * privacy risks in a dataset. It calculates an overall risk score and provides actionable recommendations.
  *
  * The overall risk assessment considers:
  * - K-Anonymity: How many records share the same quasi-identifiers
  * - L-Diversity: How diverse sensitive attributes are within equivalence classes
  * - Uniqueness: How unique individuals are based on their attributes
  */
object PrivacyRiskAnalyser {

  /** Calculates the k-anonymity score for a dataset.
    *
    * @param data The DataFrame to analyze
    * @param k The k parameter for k-anonymity
    * @param idColumn Optional user ID column to exclude from analysis
    * @return A tuple of (score, metrics) where score is 0.0-1.0 and metrics contains details
    */
  private def calculateKAnonymityScore(
      data: DataFrame,
      k: Int,
      idColumn: Option[String]
  ): (Double, Map[String, Any]) = {
    val kAnon = new KAnonymity(k)
    val kData = kAnon.apply(data, idColumn)

    val totalGroups = kData.count()
    val nonCompliantGroups = kData.filter(F.col("count") < k).count()
    val compliantGroups = totalGroups - nonCompliantGroups

    val score = if (totalGroups > 0) compliantGroups.toDouble / totalGroups.toDouble else 0.0

    val minGroupSize = if (totalGroups > 0) {
      kData.agg(F.min("count")).first().getLong(0)
    } else 0L

    val metrics = Map(
      "totalGroups" -> totalGroups,
      "compliantGroups" -> compliantGroups,
      "nonCompliantGroups" -> nonCompliantGroups,
      "minGroupSize" -> minGroupSize,
      "isKAnonymous" -> kAnon.isKAnonymous(data, idColumn)
    )

    (score, metrics)
  }

  /** Calculates the l-diversity score for a dataset.
    *
    * @param data The DataFrame to analyze
    * @param l The l parameter for l-diversity
    * @param sensitiveColumn The column containing sensitive attributes
    * @return A tuple of (score, metrics) where score is 0.0-1.0 and metrics contains details
    */
  private def calculateLDiversityScore(
      data: DataFrame,
      l: Int,
      sensitiveColumn: String
  ): (Double, Map[String, Any]) = {
    val lDiv = new LDiversity(l)
    val lData = lDiv.apply(data, sensitiveColumn)

    val totalGroups = lData.count()
    val nonCompliantGroups = lData.filter(F.col("distinctCount") < l).count()
    val compliantGroups = totalGroups - nonCompliantGroups

    val score = if (totalGroups > 0) compliantGroups.toDouble / totalGroups.toDouble else 0.0

    val minDistinctCount = if (totalGroups > 0) {
      lData.agg(F.min("distinctCount")).first().getLong(0)
    } else 0L

    val metrics = Map(
      "totalGroups" -> totalGroups,
      "compliantGroups" -> compliantGroups,
      "nonCompliantGroups" -> nonCompliantGroups,
      "minDistinctCount" -> minDistinctCount,
      "isLDiverse" -> lDiv.isLDiverse(data, sensitiveColumn)
    )

    (score, metrics)
  }

  /** Calculates the uniqueness score for a dataset.
    *
    * A lower uniqueness value indicates better privacy (less unique individuals).
    *
    * @param data The DataFrame to analyze
    * @param groupByColumns Columns to group by for uniqueness analysis
    * @param userIdColumn User ID column
    * @return A tuple of (score, metrics) where score is 0.0-1.0 and metrics contains details
    */
  private def calculateUniquenessScore(
      data: DataFrame,
      groupByColumns: Seq[String],
      userIdColumn: String
  ): (Double, Map[String, Any]) = {
    val uniquenessData = UniquenessAnalyser.apply(data, groupByColumns, userIdColumn)

    // Calculate the percentage of unique records (uniqueness = 1)
    val uniqueRecords = uniquenessData.filter(F.col("uniqueness") === 1)
    val uniqueRatio = if (uniqueRecords.count() > 0) {
      uniqueRecords.select("cumulativeValueRatio").first().getDouble(0)
    } else 0.0

    // Score is inverse of uniqueness (1.0 - uniqueRatio) because lower uniqueness is better
    val score = 1.0 - uniqueRatio

    val metrics = Map(
      "uniqueRatio" -> uniqueRatio,
      "uniquenessDistribution" -> uniquenessData
        .select("uniqueness", "value_ratio")
        .collect()
        .map(row => (row.getLong(0), row.getDouble(1)))
        .toMap
    )

    (score, metrics)
  }

  /** Determines the overall risk level based on the risk score.
    *
    * @param score Overall risk score (0.0 to 1.0, where 1.0 is safest)
    * @return Risk level: "CRITICAL", "HIGH", "MEDIUM", or "LOW"
    */
  private def determineRiskLevel(score: Double): String = {
    score match {
      case s if s >= 0.8 => "LOW"
      case s if s >= 0.6 => "MEDIUM"
      case s if s >= 0.4 => "HIGH"
      case _ => "CRITICAL"
    }
  }

  /** Generates recommendations based on the privacy analysis results.
    *
    * @param kAnonymityScore K-anonymity score
    * @param lDiversityScore L-diversity score (optional)
    * @param uniquenessScore Uniqueness score
    * @param kMetrics K-anonymity metrics
    * @param lMetrics L-diversity metrics (optional)
    * @param params Analysis parameters
    * @return List of actionable recommendations
    */
  private def generateRecommendations(
      kAnonymityScore: Double,
      lDiversityScore: Option[Double],
      uniquenessScore: Double,
      kMetrics: Map[String, Any],
      lMetrics: Option[Map[String, Any]],
      params: PrivacyRiskParams
  ): Seq[String] = {
    var recommendations = Seq.empty[String]

    // K-Anonymity recommendations
    if (kAnonymityScore < 0.8) {
      val isKAnonymous = kMetrics("isKAnonymous").asInstanceOf[Boolean]
      if (!isKAnonymous) {
        recommendations = recommendations :+ s"Dataset does not satisfy ${params.k}-anonymity. Consider increasing generalization of quasi-identifiers."
      }
      val nonCompliantGroups = kMetrics("nonCompliantGroups").asInstanceOf[Long]
      if (nonCompliantGroups > 0) {
        recommendations = recommendations :+ s"$nonCompliantGroups groups have fewer than ${params.k} records. Use removeLessThanKRows() to filter them out."
      }
    }

    // L-Diversity recommendations
    params.l.foreach { lValue =>
      lDiversityScore.foreach { score =>
        if (score < 0.8) {
          lMetrics.foreach { metrics =>
            val isLDiverse = metrics("isLDiverse").asInstanceOf[Boolean]
            if (!isLDiverse) {
              recommendations = recommendations :+ s"Dataset does not satisfy $lValue-diversity. Sensitive attribute '${params.sensitiveColumn.getOrElse("")}' lacks sufficient diversity."
            }
            val nonCompliantGroups = metrics("nonCompliantGroups").asInstanceOf[Long]
            if (nonCompliantGroups > 0) {
              recommendations = recommendations :+ s"$nonCompliantGroups groups have fewer than $lValue distinct sensitive values. Consider suppressing or generalizing these records."
            }
          }
        }
      }
    }

    // Uniqueness recommendations
    if (uniquenessScore < 0.7) {
      val uniqueRatio = uniquenessScore match {
        case s => 1.0 - s
      }
      val percentage = (uniqueRatio * 100).toInt
      recommendations = recommendations :+ s"$percentage% of attribute combinations are unique, increasing re-identification risk. Consider additional generalization."
    }

    // General recommendations
    if (recommendations.isEmpty) {
      recommendations = recommendations :+ "Privacy protection appears adequate, but continue monitoring as data evolves."
    } else {
      recommendations = recommendations :+ "Review and apply recommended anonymization strategies before releasing data."
    }

    recommendations
  }

  /** Performs a comprehensive privacy risk assessment on a dataset.
    *
    * @param data The DataFrame to analyze
    * @param params Privacy risk analysis parameters
    * @return PrivacyRiskAssessment containing scores, risk level, and recommendations
    */
  def apply(data: DataFrame, params: PrivacyRiskParams): PrivacyRiskAssessment = {
    // Calculate K-Anonymity score
    val (kScore, kMetrics) = calculateKAnonymityScore(data, params.k, Some(params.userIdColumn))

    // Calculate L-Diversity score if sensitive column is provided
    val (lScore, lMetrics) = params.l.flatMap { lValue =>
      params.sensitiveColumn.map { sensCol =>
        calculateLDiversityScore(data, lValue, sensCol)
      }
    }.unzip match {
      case (scores, metrics) => (scores.headOption, metrics.headOption)
    }

    // Calculate Uniqueness score
    val (uScore, uMetrics) = calculateUniquenessScore(data, params.groupByColumns, params.userIdColumn)

    // Calculate overall risk score (weighted average)
    val weights = lScore match {
      case Some(_) => (0.35, 0.35, 0.30) // (k-anon, l-div, uniqueness)
      case None => (0.50, 0.0, 0.50) // (k-anon, l-div, uniqueness)
    }

    val overallScore = weights._1 * kScore +
      weights._2 * lScore.getOrElse(0.0) +
      weights._3 * uScore

    // Determine risk level
    val riskLevel = determineRiskLevel(overallScore)

    // Generate recommendations
    val recommendations = generateRecommendations(kScore, lScore, uScore, kMetrics, lMetrics, params)

    // Combine all metrics
    val allMetrics = Map(
      "kAnonymity" -> kMetrics,
      "uniqueness" -> uMetrics
    ) ++ lMetrics.map(m => "lDiversity" -> m)

    PrivacyRiskAssessment(
      kAnonymityScore = kScore,
      lDiversityScore = lScore,
      uniquenessScore = uScore,
      overallRiskLevel = riskLevel,
      overallRiskScore = overallScore,
      recommendations = recommendations,
      metrics = allMetrics
    )
  }

  /** Convenience method for basic privacy risk assessment without l-diversity.
    *
    * @param data The DataFrame to analyze
    * @param k The k parameter for k-anonymity
    * @param userIdColumn User ID column
    * @param groupByColumns Columns to group by for analysis
    * @return PrivacyRiskAssessment
    */
  def apply(
      data: DataFrame,
      k: Int,
      userIdColumn: String,
      groupByColumns: Seq[String]
  ): PrivacyRiskAssessment = {
    apply(
      data,
      PrivacyRiskParams(
        k = k,
        l = None,
        userIdColumn = userIdColumn,
        sensitiveColumn = None,
        groupByColumns = groupByColumns
      )
    )
  }

  /** Convenience method for privacy risk assessment with l-diversity.
    *
    * @param data The DataFrame to analyze
    * @param k The k parameter for k-anonymity
    * @param l The l parameter for l-diversity
    * @param userIdColumn User ID column
    * @param sensitiveColumn Sensitive attribute column
    * @param groupByColumns Columns to group by for analysis
    * @return PrivacyRiskAssessment
    */
  def apply(
      data: DataFrame,
      k: Int,
      l: Int,
      userIdColumn: String,
      sensitiveColumn: String,
      groupByColumns: Seq[String]
  ): PrivacyRiskAssessment = {
    apply(
      data,
      PrivacyRiskParams(
        k = k,
        l = Some(l),
        userIdColumn = userIdColumn,
        sensitiveColumn = Some(sensitiveColumn),
        groupByColumns = groupByColumns
      )
    )
  }
}
