// Example Privacy Engineering Workflow
// This example demonstrates how to use Maskala's new privacy tools

import org.apache.spark.sql.SparkSession
import org.mitchelllisle.analysers.{PrivacyRiskScorer, RetentionAnalyser, RetentionPolicy, MerkleTree}

object PrivacyWorkflowExample {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("PrivacyWorkflow")
      .getOrCreate()

    import spark.implicits._

    // Sample user data
    val userData = Seq(
      ("user1", "25", "M", "10001", "2023-01-15", "alice@email.com"),
      ("user2", "30", "F", "10002", "2024-08-20", "bob@email.com"),
      ("user3", "35", "M", "10003", "2023-03-10", "charlie@email.com"),
      ("user4", "25", "M", "10001", "2024-09-01", "diana@email.com")
    ).toDF("user_id", "age", "gender", "zipcode", "last_activity", "email")

    // === Step 1: Assess Privacy Risks ===
    println("=== Step 1: Privacy Risk Assessment ===")
    val riskScores = PrivacyRiskScorer.calculateRiskScores(
      userData,
      quasiIdentifiers = Seq("age", "gender", "zipcode"),
      idColumn = "user_id"
    )
    
    riskScores.show()
    
    // Check if dataset meets privacy standards
    val meetsStandard = PrivacyRiskScorer.meetsPrivacyStandard(
      userData,
      Seq("age", "gender", "zipcode"),
      "user_id",
      maxAcceptableRisk = 0.5
    )
    
    println(s"Dataset meets privacy standards: $meetsStandard")

    // === Step 2: Identify Records for Retention ===
    println("\n=== Step 2: Data Retention Analysis ===")
    val retentionPolicy = RetentionPolicy(
      retentionDays = 365,
      gracePeriodDays = 30
    )
    
    val retentionResult = RetentionAnalyser.identifyRecordsForDeletion(
      userData,
      "user_id",
      "last_activity",
      retentionPolicy
    )
    
    println(s"Records for deletion: ${retentionResult.idsForDeletion.mkString(", ")}")
    println(s"Records in grace period: ${retentionResult.idsInGracePeriod.mkString(", ")}")

    // === Step 3: Create Merkle Proof Before Deletion ===
    println("\n=== Step 3: Cryptographic Verification ===")
    val beforeProof = MerkleTree.apply(
      userData,
      Seq("email", "age", "gender"),
      "user_id"
    )
    
    println(s"Before deletion - Root hash: ${beforeProof.rootHash}")
    println(s"Before deletion - Record count: ${beforeProof.recordCount}")

    // Delete records
    val afterData = userData.filter(!$"user_id".isin(retentionResult.idsForDeletion: _*))
    
    // Verify deletion
    if (retentionResult.idsForDeletion.nonEmpty) {
      val deletionProof = MerkleTree.verifyDeletion(
        userData,
        afterData,
        retentionResult.idsForDeletion,
        Seq("email", "age", "gender"),
        "user_id"
      )
      
      val isValid = MerkleTree.validateDeletionProof(
        deletionProof,
        retentionResult.totalRecordsForDeletion.toInt
      )
      
      println(s"Deletion verified: $isValid")
    }

    // === Step 4: Generate Reports ===
    println("\n=== Step 4: Privacy Reports ===")
    val riskSummary = PrivacyRiskScorer.generateRiskSummary(
      userData,
      Seq("age", "gender", "zipcode"),
      "user_id"
    )
    
    println("Risk Distribution:")
    riskSummary.show()

    spark.stop()
  }
}
