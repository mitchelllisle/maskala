package org.mitchelllisle.analysers

import org.apache.spark.sql.{DataFrame, functions => F}

case class PrivacyRiskScore(
                             individualId: String,
                             uniquenessScore: Double,
                             kAnonymityScore: Double,
                             overallRiskScore: Double,
                             riskLevel: String
                           )

/** PrivacyRiskScorer provides comprehensive privacy risk assessment for individuals in a dataset.
  * It combines multiple privacy metrics to generate an overall risk score for re-identification.
  *
  * Risk scoring helps data engineers:
  * - Identify high-risk individuals who need additional protection
  * - Prioritize privacy-enhancing transformations
  * - Make informed decisions about data publication
  * - Comply with privacy regulations by quantifying risk
  */
object PrivacyRiskScorer {

  /** Calculates a privacy risk score for each individual based on uniqueness and k-anonymity.
    *
    * The risk score combines:
    * - Uniqueness: How unique an individual's combination of attributes is
    * - K-Anonymity: How many others share the same quasi-identifiers
    *
    * @param data The DataFrame containing records to analyze
    * @param quasiIdentifiers Columns that could be used for re-identification (e.g., age, zip, gender)
    * @param idColumn Column containing unique identifiers
    * @return DataFrame with risk scores for each individual
    */
  def calculateRiskScores(
                           data: DataFrame,
                           quasiIdentifiers: Seq[String],
                           idColumn: String
                         ): DataFrame = {

    // Calculate group size (k-anonymity measure)
    val groupedData = data
      .groupBy(quasiIdentifiers.map(F.col): _*)
      .agg(
        F.count("*").alias("group_size"),
        F.collect_list(idColumn).alias("group_members")
      )

    // Join back to get group size for each record
    val dataWithGroupSize = data
      .join(groupedData, quasiIdentifiers, "left")

    // Calculate risk scores
    val scoredData = dataWithGroupSize
      .withColumn(
        "k_anonymity_score",
        // Higher k = lower risk, normalize to 0-1 scale where 1 is high risk
        F.when(F.col("group_size") === 1, 1.0) // Unique = maximum risk
          .when(F.col("group_size") >= 100, 0.0) // Large groups = minimal risk
          .otherwise(1.0 - (F.log(F.col("group_size")) / F.log(F.lit(100))))
      )
      .withColumn(
        "uniqueness_score",
        // Inverse of group size normalized to 0-1
        F.lit(1.0) / F.col("group_size")
      )
      .withColumn(
        "overall_risk_score",
        // Weighted average: k-anonymity more important than raw uniqueness
        (F.col("k_anonymity_score") * 0.6 + F.col("uniqueness_score") * 0.4)
      )
      .withColumn(
        "risk_level",
        F.when(F.col("overall_risk_score") >= 0.75, "CRITICAL")
          .when(F.col("overall_risk_score") >= 0.5, "HIGH")
          .when(F.col("overall_risk_score") >= 0.25, "MEDIUM")
          .otherwise("LOW")
      )

    scoredData.select(
      F.col(idColumn).alias("individual_id"),
      F.col("uniqueness_score"),
      F.col("k_anonymity_score"),
      F.col("overall_risk_score"),
      F.col("risk_level"),
      F.col("group_size")
    )
  }

  /** Identifies high-risk individuals who should be prioritized for privacy protection.
    *
    * @param data The DataFrame containing records to analyze
    * @param quasiIdentifiers Columns that could be used for re-identification
    * @param idColumn Column containing unique identifiers
    * @param riskThreshold Minimum risk score to be considered high-risk (default 0.5)
    * @return DataFrame containing only high-risk individuals
    */
  def identifyHighRiskIndividuals(
                                   data: DataFrame,
                                   quasiIdentifiers: Seq[String],
                                   idColumn: String,
                                   riskThreshold: Double = 0.5
                                 ): DataFrame = {

    val riskScores = calculateRiskScores(data, quasiIdentifiers, idColumn)

    riskScores
      .filter(F.col("overall_risk_score") >= riskThreshold)
      .orderBy(F.col("overall_risk_score").desc)
  }

  /** Generates a summary report of privacy risk distribution across the dataset.
    *
    * @param data The DataFrame containing records to analyze
    * @param quasiIdentifiers Columns that could be used for re-identification
    * @param idColumn Column containing unique identifiers
    * @return DataFrame with aggregated risk statistics by risk level
    */
  def generateRiskSummary(
                           data: DataFrame,
                           quasiIdentifiers: Seq[String],
                           idColumn: String
                         ): DataFrame = {

    val riskScores = calculateRiskScores(data, quasiIdentifiers, idColumn)

    val totalIndividuals = riskScores.count()

    riskScores
      .groupBy("risk_level")
      .agg(
        F.count("*").alias("individual_count"),
        F.avg("overall_risk_score").alias("avg_risk_score"),
        F.min("overall_risk_score").alias("min_risk_score"),
        F.max("overall_risk_score").alias("max_risk_score")
      )
      .withColumn("percentage", (F.col("individual_count") / F.lit(totalIndividuals)) * 100)
      .orderBy(
        F.when(F.col("risk_level") === "CRITICAL", 1)
          .when(F.col("risk_level") === "HIGH", 2)
          .when(F.col("risk_level") === "MEDIUM", 3)
          .otherwise(4)
      )
  }

  /** Calculates detailed re-identification risk metrics for comprehensive privacy assessment.
    *
    * This provides additional metrics beyond the basic risk score:
    * - Prosecutor risk: Risk when attacker knows someone is in the dataset
    * - Journalist risk: Risk when attacker has background knowledge
    * - Marketer risk: Risk when attacker is interested in the group
    *
    * @param data The DataFrame containing records to analyze
    * @param quasiIdentifiers Columns that could be used for re-identification
    * @param idColumn Column containing unique identifiers
    * @return DataFrame with detailed risk metrics
    */
  def calculateDetailedRiskMetrics(
                                    data: DataFrame,
                                    quasiIdentifiers: Seq[String],
                                    idColumn: String
                                  ): DataFrame = {

    val groupedData = data
      .groupBy(quasiIdentifiers.map(F.col): _*)
      .agg(
        F.count("*").alias("group_size"),
        F.collect_list(idColumn).alias("group_members")
      )

    val totalRecords = data.count()

    val dataWithMetrics = data
      .join(groupedData, quasiIdentifiers, "left")
      .withColumn(
        "prosecutor_risk",
        // Probability of correctly identifying someone known to be in dataset
        F.lit(1.0) / F.col("group_size")
      )
      .withColumn(
        "journalist_risk",
        // Probability of uniqueness in population (approximated by dataset)
        F.when(F.col("group_size") === 1, 0.9) // Very high if unique
          .when(F.col("group_size") <= 3, 0.5) // Moderate if small group
          .otherwise(0.1) // Low if large group
      )
      .withColumn(
        "marketer_risk",
        // Proportion of population in equivalence class
        F.col("group_size") / F.lit(totalRecords)
      )

    dataWithMetrics.select(
      F.col(idColumn).alias("individual_id"),
      F.col("group_size"),
      F.col("prosecutor_risk"),
      F.col("journalist_risk"),
      F.col("marketer_risk")
    )
  }

  /** Evaluates if the dataset meets a minimum privacy standard based on risk thresholds.
    *
    * @param data The DataFrame containing records to analyze
    * @param quasiIdentifiers Columns that could be used for re-identification
    * @param idColumn Column containing unique identifiers
    * @param maxAcceptableRisk Maximum acceptable average risk score (default 0.3)
    * @param maxHighRiskPercentage Maximum acceptable percentage of high-risk individuals (default 10%)
    * @return Boolean indicating if dataset meets privacy standards
    */
  def meetsPrivacyStandard(
                            data: DataFrame,
                            quasiIdentifiers: Seq[String],
                            idColumn: String,
                            maxAcceptableRisk: Double = 0.3,
                            maxHighRiskPercentage: Double = 0.1
                          ): Boolean = {

    val riskScores = calculateRiskScores(data, quasiIdentifiers, idColumn)
    val totalIndividuals = riskScores.count()

    // Calculate average risk
    val avgRisk = riskScores
      .select(F.avg("overall_risk_score"))
      .collect()(0)
      .getDouble(0)

    // Calculate percentage of high-risk individuals (risk >= 0.5)
    val highRiskCount = riskScores
      .filter(F.col("overall_risk_score") >= 0.5)
      .count()

    val highRiskPercentage = highRiskCount.toDouble / totalIndividuals

    avgRisk <= maxAcceptableRisk && highRiskPercentage <= maxHighRiskPercentage
  }
}
