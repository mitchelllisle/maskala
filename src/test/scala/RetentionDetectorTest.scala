import org.mitchelllisle.analysers.{RetentionDetector, RetentionPolicy}
import org.scalatest.flatspec.AnyFlatSpec
import java.time.LocalDate

class RetentionDetectorTest extends AnyFlatSpec with SparkFunSuite {
  import spark.implicits._

  // Test data with various timestamps
  val testDate = LocalDate.of(2024, 1, 1)
  
  val testData = Seq(
    ("user1", "2023-12-15"), // 17 days old (at test date)
    ("user2", "2023-11-01"), // 61 days old
    ("user3", "2023-09-01"), // 122 days old
    ("user4", "2023-06-01"), // 214 days old
    ("user5", "2023-01-01"), // 365 days old
    ("user6", "2022-06-01")  // 579 days old
  ).toDF("customer_id", "last_activity_date")

  val policy30Days = RetentionPolicy("30_DAY_POLICY", 30, Some("Retain data for 30 days"))
  val policy90Days = RetentionPolicy("90_DAY_POLICY", 90, Some("Retain data for 90 days"))
  val policy365Days = RetentionPolicy("365_DAY_POLICY", 365, Some("Retain data for 1 year"))

  "detectViolations" should "identify records past retention date" in {
    val violations = RetentionDetector.detectViolations(
      testData,
      "customer_id",
      "last_activity_date",
      policy30Days,
      Some(testDate)
    )

    assert(violations.count() == 5) // All except user1 (17 days old)
    
    val customerIds = violations.select("customerId").collect().map(_.getString(0)).toSet
    assert(!customerIds.contains("user1"))
    assert(customerIds.contains("user2"))
    assert(customerIds.contains("user6"))
  }

  "detectViolations" should "calculate correct days past retention" in {
    val violations = RetentionDetector.detectViolations(
      testData,
      "customer_id",
      "last_activity_date",
      policy30Days,
      Some(testDate)
    )

    val user2Violation = violations.filter($"customerId" === "user2").first()
    // user2 is 61 days old, policy is 30 days, so 31 days past retention
    assert(user2Violation.getInt(user2Violation.fieldIndex("daysPastRetention")) == 31)
  }

  "detectViolations" should "include policy name in results" in {
    val violations = RetentionDetector.detectViolations(
      testData,
      "customer_id",
      "last_activity_date",
      policy90Days,
      Some(testDate)
    )

    violations.collect().foreach { row =>
      assert(row.getString(row.fieldIndex("policyName")) == "90_DAY_POLICY")
    }
  }

  "detectViolations" should "handle different retention periods correctly" in {
    val violations30 = RetentionDetector.detectViolations(
      testData, "customer_id", "last_activity_date", policy30Days, Some(testDate)
    )
    val violations90 = RetentionDetector.detectViolations(
      testData, "customer_id", "last_activity_date", policy90Days, Some(testDate)
    )
    val violations365 = RetentionDetector.detectViolations(
      testData, "customer_id", "last_activity_date", policy365Days, Some(testDate)
    )

    assert(violations30.count() > violations90.count())
    assert(violations90.count() > violations365.count())
  }

  "detectViolations" should "handle empty results when no violations exist" in {
    val recentData = Seq(
      ("user1", "2023-12-31"),
      ("user2", "2023-12-30")
    ).toDF("customer_id", "last_activity_date")

    val violations = RetentionDetector.detectViolations(
      recentData,
      "customer_id",
      "last_activity_date",
      policy30Days,
      Some(testDate)
    )

    assert(violations.count() == 0)
  }

  "detectViolationsWithSummary" should "return violations and summary statistics" in {
    val (violations, summary) = RetentionDetector.detectViolationsWithSummary(
      testData,
      "customer_id",
      "last_activity_date",
      policy30Days,
      Some(testDate)
    )

    assert(violations.count() == 5)
    assert(summary.totalRecords == 6)
    assert(summary.violationsFound == 5)
    assert(summary.policyName == "30_DAY_POLICY")
    assert(summary.retentionDays == 30)
    assert(summary.averageDaysPastRetention > 0)
  }

  "RetentionSummary" should "calculate compliance rate correctly" in {
    val summary = RetentionDetector.detectViolationsWithSummary(
      testData,
      "customer_id",
      "last_activity_date",
      policy30Days,
      Some(testDate)
    )._2

    val expectedComplianceRate = ((6.0 - 5.0) / 6.0) * 100.0
    assert(math.abs(summary.complianceRate - expectedComplianceRate) < 0.01)
  }

  "groupViolationsByRange" should "categorize violations correctly" in {
    val violations = RetentionDetector.detectViolations(
      testData,
      "customer_id",
      "last_activity_date",
      policy30Days,
      Some(testDate)
    )

    val grouped = RetentionDetector.groupViolationsByRange(violations)

    assert(grouped.count() > 0)
    val ranges = grouped.select("violationRange").collect().map(_.getString(0)).toSet
    assert(ranges.contains("0-30 days") || ranges.contains("31-90 days"))
  }

  "apply" should "work as shorthand for detectViolations" in {
    val violations1 = RetentionDetector.apply(
      testData,
      "customer_id",
      "last_activity_date",
      policy30Days
    )

    val violations2 = RetentionDetector.detectViolations(
      testData,
      "customer_id",
      "last_activity_date",
      policy30Days,
      None
    )

    assert(violations1.count() == violations2.count())
  }

  "detectMultiplePolicies" should "detect violations across multiple policies" in {
    val policies = Seq(policy30Days, policy90Days, policy365Days)
    
    val violations = RetentionDetector.detectMultiplePolicies(
      testData,
      "customer_id",
      "last_activity_date",
      policies
    )

    // Should have violations from all policies, but deduplicated
    assert(violations.count() > 0)
    
    val policyNames = violations.select("policyName").distinct().collect().map(_.getString(0)).toSet
    assert(policyNames.size <= 3)
  }

  "generateActionPlan" should "add recommended actions based on thresholds" in {
    val violations = RetentionDetector.detectViolations(
      testData,
      "customer_id",
      "last_activity_date",
      policy30Days,
      Some(testDate)
    )

    val actionPlan = RetentionDetector.generateActionPlan(violations, actionThresholdDays = 30)

    assert(actionPlan.columns.contains("recommendedAction"))
    assert(actionPlan.columns.contains("priority"))
    
    val actions = actionPlan.select("recommendedAction").distinct().collect().map(_.getString(0)).toSet
    assert(actions.nonEmpty)
  }

  "generateActionPlan" should "assign correct priority levels" in {
    val violations = RetentionDetector.detectViolations(
      testData,
      "customer_id",
      "last_activity_date",
      policy30Days,
      Some(testDate)
    )

    val actionPlan = RetentionDetector.generateActionPlan(violations, actionThresholdDays = 30)
    
    val priorities = actionPlan.select("priority").distinct().collect().map(_.getString(0)).toSet
    assert(priorities.exists(p => Seq("High", "Medium", "Low").contains(p)))
  }

  "detectViolations" should "handle timestamp column type" in {
    val timestampData = Seq(
      ("user1", java.sql.Timestamp.valueOf("2023-11-01 10:00:00")),
      ("user2", java.sql.Timestamp.valueOf("2023-06-01 15:30:00"))
    ).toDF("customer_id", "last_activity_ts")

    val violations = RetentionDetector.detectViolations(
      timestampData,
      "customer_id",
      "last_activity_ts",
      policy30Days,
      Some(testDate)
    )

    assert(violations.count() == 2)
  }

  "detectViolations" should "handle date column type" in {
    val dateData = Seq(
      ("user1", java.sql.Date.valueOf("2023-11-01")),
      ("user2", java.sql.Date.valueOf("2023-06-01"))
    ).toDF("customer_id", "last_activity_date")

    val violations = RetentionDetector.detectViolations(
      dateData,
      "customer_id",
      "last_activity_date",
      policy30Days,
      Some(testDate)
    )

    assert(violations.count() == 2)
  }

  "RetentionSummary toString" should "format output correctly" in {
    val summary = RetentionDetector.detectViolationsWithSummary(
      testData,
      "customer_id",
      "last_activity_date",
      policy30Days,
      Some(testDate)
    )._2

    val output = summary.toString
    assert(output.contains("30_DAY_POLICY"))
    assert(output.contains("Total Records"))
    assert(output.contains("Compliance Rate"))
  }

  "detectViolations" should "return correct lastActivity format" in {
    val violations = RetentionDetector.detectViolations(
      testData,
      "customer_id",
      "last_activity_date",
      policy30Days,
      Some(testDate)
    )

    violations.collect().foreach { row =>
      val lastActivity = row.getString(row.fieldIndex("lastActivity"))
      assert(lastActivity.nonEmpty)
    }
  }
}
