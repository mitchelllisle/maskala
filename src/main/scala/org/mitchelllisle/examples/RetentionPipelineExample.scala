package org.mitchelllisle.examples

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.mitchelllisle.analysers.{RetentionDetector, RetentionPolicy, MerkleTree}
import org.mitchelllisle.Anonymiser
import java.time.LocalDate

/** Example pipeline demonstrating data retention detection and automated cleanup.
 * 
 * This example shows how to:
 * 1. Detect retention violations
 * 2. Generate action plans
 * 3. Apply anonymization or deletion
 * 4. Create audit trails
 */
object RetentionPipelineExample {
  
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("RetentionPipelineExample")
      .master("local[*]")
      .getOrCreate()
    
    import spark.implicits._
    
    // Step 1: Load customer data
    println("=== Step 1: Loading Customer Data ===")
    val customerData = spark.read
      .option("header", "true")
      .csv("path/to/customer-data.csv")
    
    val totalRecords = customerData.count()
    println(s"Loaded $totalRecords customer records")
    
    // Step 2: Define retention policies
    println("\n=== Step 2: Defining Retention Policies ===")
    val policies = Seq(
      RetentionPolicy(
        name = "GDPR_INACTIVE_USERS",
        retentionDays = 365,
        description = Some("GDPR: Delete inactive users after 1 year")
      ),
      RetentionPolicy(
        name = "CCPA_MARKETING",
        retentionDays = 180,
        description = Some("CCPA: Marketing data retention 6 months")
      )
    )
    
    policies.foreach(p => println(s"  - ${p.name}: ${p.retentionDays} days"))
    
    // Step 3: Detect violations for primary policy
    println("\n=== Step 3: Detecting Retention Violations ===")
    val primaryPolicy = policies.head
    val (violations, summary) = RetentionDetector.detectViolationsWithSummary(
      customerData,
      idColumn = "customer_id",
      timestampColumn = "last_activity_date",
      policy = primaryPolicy
    )
    
    println(summary)
    
    if (violations.count() == 0) {
      println("✓ No violations found. All data is within retention policy.")
      spark.stop()
      return
    }
    
    // Step 4: Analyze violation patterns
    println("\n=== Step 4: Analyzing Violation Patterns ===")
    val violationsByRange = RetentionDetector.groupViolationsByRange(violations)
    println("Violations by time range:")
    violationsByRange.show(false)
    
    // Step 5: Generate action plan
    println("\n=== Step 5: Generating Action Plan ===")
    val actionPlan = RetentionDetector.generateActionPlan(
      violations,
      actionThresholdDays = 30
    )
    
    val actionSummary = actionPlan
      .groupBy("recommendedAction", "priority")
      .count()
      .orderBy("priority", "recommendedAction")
    
    println("Action plan summary:")
    actionSummary.show(false)
    
    // Step 6: Create "before" audit proof
    println("\n=== Step 6: Creating Pre-Cleanup Audit Proof ===")
    val beforeProof = MerkleTree.apply(
      customerData,
      columns = Seq("customer_id", "email", "last_activity_date"),
      idColumn = "customer_id"
    )
    println(s"Before cleanup - Root hash: ${beforeProof.rootHash}")
    println(s"Before cleanup - Record count: ${beforeProof.recordCount}")
    
    // Step 7: Execute cleanup based on action plan
    println("\n=== Step 7: Executing Cleanup Actions ===")
    
    // 7a. Delete high-priority records
    val toDelete = actionPlan
      .filter($"recommendedAction" === "Delete" && $"priority" === "High")
      .select("customerId")
    
    val afterDeletion = customerData.join(
      toDelete,
      customerData("customer_id") === toDelete("customerId"),
      "left_anti"
    )
    
    val deletedCount = totalRecords - afterDeletion.count()
    println(s"  Deleted $deletedCount high-priority records")
    
    // 7b. Anonymize medium-priority records
    val toAnonymize = actionPlan
      .filter($"recommendedAction" === "Anonymize")
      .select("customerId")
      .collect()
      .map(_.getString(0))
    
    val afterAnonymization = if (toAnonymize.nonEmpty) {
      // Apply anonymization transformations
      val anonymized = afterDeletion
        .withColumn("email",
          when($"customer_id".isin(toAnonymize: _*), 
               concat(lit("anon_"), sha2($"email", 256)))
          .otherwise($"email")
        )
        .withColumn("phone",
          when($"customer_id".isin(toAnonymize: _*), lit("REDACTED"))
          .otherwise($"phone")
        )
      
      println(s"  Anonymized ${toAnonymize.length} medium-priority records")
      anonymized
    } else {
      afterDeletion
    }
    
    // Step 8: Create "after" audit proof and verify
    println("\n=== Step 8: Creating Post-Cleanup Audit Proof ===")
    val afterProof = MerkleTree.apply(
      afterAnonymization,
      columns = Seq("customer_id", "email", "last_activity_date"),
      idColumn = "customer_id"
    )
    println(s"After cleanup - Root hash: ${afterProof.rootHash}")
    println(s"After cleanup - Record count: ${afterProof.recordCount}")
    
    // Step 9: Verify deletion if records were deleted
    if (deletedCount > 0) {
      println("\n=== Step 9: Verifying Deletions ===")
      val deletedIds = toDelete.collect().map(_.getString(0))
      val deletionProof = MerkleTree.verifyDeletion(
        beforeData = customerData,
        afterData = afterAnonymization,
        deletedIds = deletedIds,
        columns = Seq("customer_id", "email", "last_activity_date"),
        idColumn = "customer_id"
      )
      
      val isValid = MerkleTree.validateDeletionProof(
        deletionProof,
        expectedDeletions = deletedCount.toInt
      )
      
      if (isValid) {
        println("✓ Deletion cryptographically verified")
      } else {
        println("✗ Deletion verification failed!")
      }
    }
    
    // Step 10: Save cleaned data and audit logs
    println("\n=== Step 10: Saving Results ===")
    afterAnonymization.write
      .mode("overwrite")
      .parquet("output/cleaned-customer-data")
    
    // Save audit log
    val auditLog = Seq(
      (
        beforeProof.rootHash,
        afterProof.rootHash,
        beforeProof.recordCount,
        afterProof.recordCount,
        deletedCount,
        toAnonymize.length,
        java.time.Instant.now().toString
      )
    ).toDF(
      "before_hash",
      "after_hash", 
      "before_count",
      "after_count",
      "deleted_count",
      "anonymized_count",
      "timestamp"
    )
    
    auditLog.write
      .mode("append")
      .parquet("output/retention-audit-log")
    
    println("✓ Cleaned data saved to: output/cleaned-customer-data")
    println("✓ Audit log saved to: output/retention-audit-log")
    
    // Step 11: Re-check compliance
    println("\n=== Step 11: Verifying Compliance ===")
    val (finalViolations, finalSummary) = RetentionDetector.detectViolationsWithSummary(
      afterAnonymization,
      idColumn = "customer_id",
      timestampColumn = "last_activity_date",
      policy = primaryPolicy
    )
    
    println(finalSummary)
    
    if (finalViolations.count() == 0) {
      println("✓ SUCCESS: All data now complies with retention policy!")
    } else {
      println(s"⚠ WARNING: ${finalViolations.count()} violations remain (likely low-priority)")
    }
    
    spark.stop()
  }
}
