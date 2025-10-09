import org.mitchelllisle.analysers.{RetentionAnalyser, RetentionPolicy}
import org.scalatest.flatspec.AnyFlatSpec
import java.time.LocalDate
import java.sql.Date

class RetentionAnalyserTest extends AnyFlatSpec with SparkFunSuite {
  import spark.implicits._

  val testData = Seq(
    ("user1", Date.valueOf("2023-01-01")),  // Very old
    ("user2", Date.valueOf("2023-06-01")),  // Old
    ("user3", Date.valueOf("2024-08-01")),  // Recent
    ("user4", Date.valueOf("2024-09-01")),  // Very recent
    ("user5", Date.valueOf("2023-03-15"))   // Old
  ).toDF("user_id", "last_activity_date")

  val currentDate = LocalDate.of(2024, 10, 1)
  val policy = RetentionPolicy(retentionDays = 365, gracePeriodDays = 30)

  "identifyRecordsForDeletion" should "correctly identify records past retention period" in {
    val result = RetentionAnalyser.identifyRecordsForDeletion(
      testData,
      "user_id",
      "last_activity_date",
      policy,
      currentDate
    )

    // Records from 2023 should be flagged for deletion (> 365 + 30 days old)
    assert(result.totalRecordsForDeletion > 0)
    assert(result.idsForDeletion.contains("user1"))
    assert(!result.idsForDeletion.contains("user3")) // Recent records should not be flagged
    assert(!result.idsForDeletion.contains("user4"))
  }

  "identifyRecordsForDeletion" should "identify records in grace period" in {
    val result = RetentionAnalyser.identifyRecordsForDeletion(
      testData,
      "user_id",
      "last_activity_date",
      policy,
      currentDate
    )

    // Should have proper counts
    assert(result.totalRecordsInGracePeriod >= 0)
    assert(result.idsInGracePeriod.length == result.totalRecordsInGracePeriod)
  }

  "identifyRecordsForDeletion" should "have timestamp" in {
    val result = RetentionAnalyser.identifyRecordsForDeletion(
      testData,
      "user_id",
      "last_activity_date",
      policy,
      currentDate
    )

    assert(result.analysisTimestamp != null)
  }

  "identifyRecordsForDeletionMultiDate" should "use most recent date across columns" in {
    val multiDateData = Seq(
      ("user1", Date.valueOf("2023-01-01"), Date.valueOf("2024-09-01")), // Recent activity
      ("user2", Date.valueOf("2023-01-01"), Date.valueOf("2023-02-01")), // All old
      ("user3", Date.valueOf("2024-08-01"), Date.valueOf("2024-09-01"))  // All recent
    ).toDF("user_id", "created_date", "last_login_date")

    val result = RetentionAnalyser.identifyRecordsForDeletionMultiDate(
      multiDateData,
      "user_id",
      Seq("created_date", "last_login_date"),
      policy,
      currentDate
    )

    // user1 should NOT be flagged because last_login_date is recent
    assert(!result.idsForDeletion.contains("user1"))
    // user2 should be flagged because both dates are old
    assert(result.idsForDeletion.contains("user2"))
    // user3 should NOT be flagged
    assert(!result.idsForDeletion.contains("user3"))
  }

  "createRetentionSummary" should "create summary with retention statuses" in {
    val summary = RetentionAnalyser.createRetentionSummary(
      testData,
      "user_id",
      "last_activity_date",
      policy,
      currentDate
    )

    assert(summary.columns.contains("retention_status"))
    assert(summary.columns.contains("unique_ids"))
    assert(summary.columns.contains("total_records"))
    assert(summary.columns.contains("oldest_date"))
    assert(summary.columns.contains("newest_date"))

    val statuses = summary.select("retention_status").collect().map(_.getString(0))
    // Should have at least some categorization
    assert(statuses.nonEmpty)
  }

  "createRetentionSummary" should "aggregate counts correctly" in {
    val summary = RetentionAnalyser.createRetentionSummary(
      testData,
      "user_id",
      "last_activity_date",
      policy,
      currentDate
    )

    val totalUniqueIds = summary.select("unique_ids").collect().map(_.getLong(0)).sum
    // Should account for all users in test data
    assert(totalUniqueIds == 5)
  }

  "RetentionPolicy" should "support zero grace period" in {
    val noGracePolicy = RetentionPolicy(retentionDays = 365, gracePeriodDays = 0)
    
    val result = RetentionAnalyser.identifyRecordsForDeletion(
      testData,
      "user_id",
      "last_activity_date",
      noGracePolicy,
      currentDate
    )

    // Should still work with zero grace period
    assert(result.totalRecordsForDeletion >= 0)
    assert(result.totalRecordsInGracePeriod == 0) // No grace period
  }

  "identifyRecordsForDeletion" should "handle empty DataFrame" in {
    val emptyData = testData.filter($"user_id" === "nonexistent")
    
    val result = RetentionAnalyser.identifyRecordsForDeletion(
      emptyData,
      "user_id",
      "last_activity_date",
      policy,
      currentDate
    )

    assert(result.totalRecordsForDeletion == 0)
    assert(result.totalRecordsInGracePeriod == 0)
    assert(result.idsForDeletion.isEmpty)
    assert(result.idsInGracePeriod.isEmpty)
  }

  "identifyRecordsForDeletion" should "not include same record in both lists" in {
    val result = RetentionAnalyser.identifyRecordsForDeletion(
      testData,
      "user_id",
      "last_activity_date",
      policy,
      currentDate
    )

    val intersection = result.idsForDeletion.toSet.intersect(result.idsInGracePeriod.toSet)
    assert(intersection.isEmpty, "Records should not appear in both deletion and grace period lists")
  }
}
