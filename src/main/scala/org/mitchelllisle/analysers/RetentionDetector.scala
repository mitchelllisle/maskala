package org.mitchelllisle.analysers

import org.apache.spark.sql.{DataFrame, functions => F}
import org.apache.spark.sql.types._
import java.time.{Instant, LocalDate}
import java.time.temporal.ChronoUnit

/** Represents a retention policy defining how long data should be kept.
 *
 * @param name A descriptive name for the policy (e.g., "GDPR_DEFAULT", "ACTIVE_USERS")
 * @param retentionDays Number of days to retain data
 * @param description Optional description of the policy
 */
case class RetentionPolicy(
  name: String,
  retentionDays: Int,
  description: Option[String] = None
)

/** Represents a customer or record that has exceeded its retention schedule.
 *
 * @param customerId The unique identifier for the customer/record
 * @param daysPastRetention Number of days beyond the retention policy
 * @param lastActivity The timestamp or date of last activity
 * @param policyName The retention policy that was violated
 */
case class RetentionViolation(
  customerId: String,
  daysPastRetention: Long,
  lastActivity: String,
  policyName: String
)

/** Configuration parameters for retention detection analysis.
 *
 * @param idColumn The column containing customer/record identifiers
 * @param timestampColumn The column containing the timestamp or date to check against retention
 * @param policyName The name of the retention policy to apply
 */
case class RetentionParams(
  idColumn: String,
  timestampColumn: String,
  policyName: String
) extends AnalyserParams

/** Analyzes datasets to detect records that exceed specified retention schedules.
 * 
 * This analyzer helps ensure compliance with data retention policies (GDPR, CCPA, etc.)
 * by identifying customers whose data should be deleted or anonymized.
 */
object RetentionDetector {

  /** Detects records that have exceeded their retention schedule.
   *
   * @param data The DataFrame to analyze
   * @param idColumn The column containing unique identifiers
   * @param timestampColumn The column containing timestamps or dates to check
   * @param policy The retention policy to apply
   * @param referenceDate Optional reference date for checking (defaults to current date)
   * @return DataFrame containing records that exceed the retention policy
   */
  def detectViolations(
    data: DataFrame,
    idColumn: String,
    timestampColumn: String,
    policy: RetentionPolicy,
    referenceDate: Option[LocalDate] = None
  ): DataFrame = {
    val refDate = referenceDate.getOrElse(LocalDate.now())
    val retentionCutoffDate = refDate.minusDays(policy.retentionDays)
    
    // Convert reference date to timestamp for comparison
    val cutoffTimestamp = F.lit(java.sql.Timestamp.valueOf(retentionCutoffDate.atStartOfDay()))
    
    // Determine the type of timestamp column and convert accordingly
    val timestampExpr = data.schema(timestampColumn).dataType match {
      case _: TimestampType => F.col(timestampColumn)
      case _: DateType => F.col(timestampColumn).cast(TimestampType)
      case _: StringType => F.to_timestamp(F.col(timestampColumn))
      case _ => throw new IllegalArgumentException(
        s"Column '$timestampColumn' must be of type Timestamp, Date, or String"
      )
    }
    
    // Calculate days past retention
    val daysPastRetention = F.datediff(
      F.lit(java.sql.Date.valueOf(retentionCutoffDate)),
      timestampExpr
    )
    
    // Filter records that are past retention
    data
      .filter(timestampExpr < cutoffTimestamp)
      .select(
        F.col(idColumn).alias("customerId"),
        daysPastRetention.alias("daysPastRetention"),
        timestampExpr.cast(StringType).alias("lastActivity"),
        F.lit(policy.name).alias("policyName")
      )
      .filter(F.col("daysPastRetention") > 0)
  }

  /** Detects violations and returns a summary with statistics.
   *
   * @param data The DataFrame to analyze
   * @param idColumn The column containing unique identifiers
   * @param timestampColumn The column containing timestamps or dates to check
   * @param policy The retention policy to apply
   * @param referenceDate Optional reference date for checking (defaults to current date)
   * @return Tuple of (violations DataFrame, summary statistics)
   */
  def detectViolationsWithSummary(
    data: DataFrame,
    idColumn: String,
    timestampColumn: String,
    policy: RetentionPolicy,
    referenceDate: Option[LocalDate] = None
  ): (DataFrame, RetentionSummary) = {
    val violations = detectViolations(data, idColumn, timestampColumn, policy, referenceDate)
    val totalRecords = data.count()
    val violationCount = violations.count()
    val avgDaysPast = if (violationCount > 0) {
      violations.select(F.avg("daysPastRetention")).first().getDouble(0)
    } else {
      0.0
    }
    
    val summary = RetentionSummary(
      policyName = policy.name,
      totalRecords = totalRecords,
      violationsFound = violationCount,
      averageDaysPastRetention = avgDaysPast,
      retentionDays = policy.retentionDays
    )
    
    (violations, summary)
  }

  /** Groups violations by time ranges for better insight.
   *
   * @param violations DataFrame containing retention violations
   * @return DataFrame with violations grouped by time ranges
   */
  def groupViolationsByRange(violations: DataFrame): DataFrame = {
    violations
      .withColumn("violationRange",
        F.when(F.col("daysPastRetention") <= 30, "0-30 days")
          .when(F.col("daysPastRetention") <= 90, "31-90 days")
          .when(F.col("daysPastRetention") <= 180, "91-180 days")
          .when(F.col("daysPastRetention") <= 365, "181-365 days")
          .otherwise("Over 1 year")
      )
      .groupBy("violationRange", "policyName")
      .agg(
        F.count("customerId").alias("recordCount"),
        F.avg("daysPastRetention").alias("avgDaysPast"),
        F.max("daysPastRetention").alias("maxDaysPast")
      )
      .orderBy("violationRange")
  }

  /** Applies the standard detection workflow with a single policy.
   *
   * @param data The DataFrame to analyze
   * @param idColumn The column containing unique identifiers
   * @param timestampColumn The column containing timestamps or dates to check
   * @param policy The retention policy to apply
   * @return DataFrame containing records that exceed the retention policy
   */
  def apply(
    data: DataFrame,
    idColumn: String,
    timestampColumn: String,
    policy: RetentionPolicy
  ): DataFrame = {
    detectViolations(data, idColumn, timestampColumn, policy)
  }

  /** Detects violations across multiple retention policies.
   *
   * @param data The DataFrame to analyze
   * @param idColumn The column containing unique identifiers
   * @param timestampColumn The column containing timestamps or dates to check
   * @param policies Sequence of retention policies to check
   * @return DataFrame containing all violations across all policies
   */
  def detectMultiplePolicies(
    data: DataFrame,
    idColumn: String,
    timestampColumn: String,
    policies: Seq[RetentionPolicy]
  ): DataFrame = {
    policies.map { policy =>
      detectViolations(data, idColumn, timestampColumn, policy)
    }.reduce(_ union _).distinct()
  }

  /** Generates an anonymization/deletion action plan for violations.
   *
   * @param violations DataFrame containing retention violations
   * @param actionThresholdDays Number of days past retention before action is recommended
   * @return DataFrame with recommended actions
   */
  def generateActionPlan(
    violations: DataFrame,
    actionThresholdDays: Int = 30
  ): DataFrame = {
    violations
      .withColumn("recommendedAction",
        F.when(F.col("daysPastRetention") < actionThresholdDays, "Review")
          .when(F.col("daysPastRetention") < actionThresholdDays * 2, "Anonymize")
          .otherwise("Delete")
      )
      .withColumn("priority",
        F.when(F.col("daysPastRetention") >= actionThresholdDays * 3, "High")
          .when(F.col("daysPastRetention") >= actionThresholdDays * 2, "Medium")
          .otherwise("Low")
      )
  }
}

/** Summary statistics for retention detection results.
 *
 * @param policyName The name of the retention policy applied
 * @param totalRecords Total number of records analyzed
 * @param violationsFound Number of records exceeding retention
 * @param averageDaysPastRetention Average days past the retention schedule
 * @param retentionDays The retention period in days
 */
case class RetentionSummary(
  policyName: String,
  totalRecords: Long,
  violationsFound: Long,
  averageDaysPastRetention: Double,
  retentionDays: Int
) {
  def complianceRate: Double = {
    if (totalRecords == 0) 100.0
    else ((totalRecords - violationsFound).toDouble / totalRecords) * 100.0
  }
  
  override def toString: String = {
    s"""
       |Retention Policy: $policyName
       |Retention Period: $retentionDays days
       |Total Records: $totalRecords
       |Violations Found: $violationsFound
       |Compliance Rate: ${f"$complianceRate%.2f"}%
       |Average Days Past Retention: ${f"$averageDaysPastRetention%.1f"}
       |""".stripMargin
  }
}
