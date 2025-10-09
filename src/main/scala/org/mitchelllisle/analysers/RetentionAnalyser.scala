package org.mitchelllisle.analysers

import org.apache.spark.sql.{DataFrame, functions => F}
import java.time.{Instant, LocalDate, ZoneOffset}
import java.sql.Date

case class RetentionPolicy(
                            retentionDays: Int,
                            gracePeriodDays: Int = 0
                          )

case class RetentionResult(
                            idsForDeletion: Seq[String],
                            idsInGracePeriod: Seq[String],
                            totalRecordsForDeletion: Long,
                            totalRecordsInGracePeriod: Long,
                            analysisTimestamp: Instant = Instant.now()
                          )

/** RetentionAnalyser helps identify records that need to be deleted based on retention policies.
  * This is essential for compliance with GDPR, CCPA, and other data privacy regulations.
  * 
  * Key use cases:
  * - Identify customer records that have exceeded retention periods
  * - Provide grace periods before actual deletion
  * - Support for multiple date-based retention criteria
  */
object RetentionAnalyser {

  /** Identifies records that should be deleted based on a retention policy.
    *
    * @param data The DataFrame containing records to analyze
    * @param idColumn Column containing unique identifiers
    * @param dateColumn Column containing the date to evaluate retention against (e.g., last_activity_date, created_date)
    * @param policy RetentionPolicy defining retention period and grace period
    * @param currentDate Optional current date for testing (defaults to today)
    * @return RetentionResult with IDs for deletion and in grace period
    */
  def identifyRecordsForDeletion(
                                  data: DataFrame,
                                  idColumn: String,
                                  dateColumn: String,
                                  policy: RetentionPolicy,
                                  currentDate: LocalDate = LocalDate.now()
                                ): RetentionResult = {

    val retentionCutoffDate = currentDate.minusDays(policy.retentionDays)
    val gracePeriodCutoffDate = currentDate.minusDays(policy.retentionDays + policy.gracePeriodDays)

    // Convert LocalDate to sql.Date for comparison
    val retentionCutoff = Date.valueOf(retentionCutoffDate)
    val gracePeriodCutoff = Date.valueOf(gracePeriodCutoffDate)

    // Records that are past retention period (eligible for deletion)
    val recordsForDeletion = data
      .filter(F.col(dateColumn) < F.lit(gracePeriodCutoff))
      .select(idColumn)
      .distinct()

    // Records in grace period (warning - will be deleted soon)
    val recordsInGracePeriod = data
      .filter(
        F.col(dateColumn) >= F.lit(gracePeriodCutoff) &&
          F.col(dateColumn) < F.lit(retentionCutoff)
      )
      .select(idColumn)
      .distinct()

    val idsForDeletion = recordsForDeletion.collect().map(_.getString(0)).toSeq
    val idsInGracePeriod = recordsInGracePeriod.collect().map(_.getString(0)).toSeq

    RetentionResult(
      idsForDeletion = idsForDeletion,
      idsInGracePeriod = idsInGracePeriod,
      totalRecordsForDeletion = idsForDeletion.length,
      totalRecordsInGracePeriod = idsInGracePeriod.length,
      analysisTimestamp = Instant.now()
    )
  }

  /** Analyzes retention across multiple date columns and returns the most conservative result.
    * Useful when you have multiple date criteria (e.g., last_login, last_purchase, account_created).
    *
    * @param data The DataFrame containing records to analyze
    * @param idColumn Column containing unique identifiers
    * @param dateColumns Multiple date columns to check
    * @param policy RetentionPolicy defining retention period
    * @param currentDate Optional current date for testing
    * @return RetentionResult considering all date columns (keeps records if ANY date is recent)
    */
  def identifyRecordsForDeletionMultiDate(
                                           data: DataFrame,
                                           idColumn: String,
                                           dateColumns: Seq[String],
                                           policy: RetentionPolicy,
                                           currentDate: LocalDate = LocalDate.now()
                                         ): RetentionResult = {

    require(dateColumns.nonEmpty, "Must provide at least one date column")

    val retentionCutoffDate = currentDate.minusDays(policy.retentionDays)
    val gracePeriodCutoffDate = currentDate.minusDays(policy.retentionDays + policy.gracePeriodDays)

    val retentionCutoff = Date.valueOf(retentionCutoffDate)
    val gracePeriodCutoff = Date.valueOf(gracePeriodCutoffDate)

    // Find the most recent date across all date columns for each record
    val mostRecentDateCol = dateColumns
      .map(F.col)
      .reduce((col1, col2) => F.greatest(col1, col2))

    val dataWithMostRecentDate = data
      .withColumn("most_recent_date", mostRecentDateCol)

    // Records past retention (all dates are old)
    val recordsForDeletion = dataWithMostRecentDate
      .filter(F.col("most_recent_date") < F.lit(gracePeriodCutoff))
      .select(idColumn)
      .distinct()

    // Records in grace period
    val recordsInGracePeriod = dataWithMostRecentDate
      .filter(
        F.col("most_recent_date") >= F.lit(gracePeriodCutoff) &&
          F.col("most_recent_date") < F.lit(retentionCutoff)
      )
      .select(idColumn)
      .distinct()

    val idsForDeletion = recordsForDeletion.collect().map(_.getString(0)).toSeq
    val idsInGracePeriod = recordsInGracePeriod.collect().map(_.getString(0)).toSeq

    RetentionResult(
      idsForDeletion = idsForDeletion,
      idsInGracePeriod = idsInGracePeriod,
      totalRecordsForDeletion = idsForDeletion.length,
      totalRecordsInGracePeriod = idsInGracePeriod.length,
      analysisTimestamp = Instant.now()
    )
  }

  /** Creates a DataFrame summary of retention analysis with detailed statistics.
    *
    * @param data The DataFrame containing records to analyze
    * @param idColumn Column containing unique identifiers
    * @param dateColumn Column containing the date to evaluate
    * @param policy RetentionPolicy defining retention period
    * @param currentDate Optional current date for testing
    * @return DataFrame with retention statistics by time period
    */
  def createRetentionSummary(
                              data: DataFrame,
                              idColumn: String,
                              dateColumn: String,
                              policy: RetentionPolicy,
                              currentDate: LocalDate = LocalDate.now()
                            ): DataFrame = {

    val retentionCutoffDate = currentDate.minusDays(policy.retentionDays)
    val gracePeriodCutoffDate = currentDate.minusDays(policy.retentionDays + policy.gracePeriodDays)

    val retentionCutoff = Date.valueOf(retentionCutoffDate)
    val gracePeriodCutoff = Date.valueOf(gracePeriodCutoffDate)

    data
      .withColumn(
        "retention_status",
        F.when(F.col(dateColumn) < F.lit(gracePeriodCutoff), "FOR_DELETION")
          .when(F.col(dateColumn) < F.lit(retentionCutoff), "GRACE_PERIOD")
          .otherwise("RETAINED")
      )
      .groupBy("retention_status")
      .agg(
        F.countDistinct(idColumn).alias("unique_ids"),
        F.count("*").alias("total_records"),
        F.min(dateColumn).alias("oldest_date"),
        F.max(dateColumn).alias("newest_date")
      )
      .orderBy("retention_status")
  }
}
