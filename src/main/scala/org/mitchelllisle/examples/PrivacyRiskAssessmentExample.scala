package org.mitchelllisle.examples

import org.apache.spark.sql.SparkSession
import org.mitchelllisle.analysers.{PrivacyRiskAssessment, PrivacyRiskParams}

/** Example demonstrating how to use the Privacy Risk Assessment module.
  *
  * This example shows how to:
  * 1. Detect quasi-identifiers in a dataset
  * 2. Perform a comprehensive privacy risk assessment
  * 3. Generate and review recommendations
  */
object PrivacyRiskAssessmentExample {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("Privacy Risk Assessment Example")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    // Example 1: Basic Privacy Risk Assessment
    println("=" * 80)
    println("Example 1: Basic Privacy Risk Assessment")
    println("=" * 80)

    val patientData = Seq(
      ("1", "30", "Male", "12345", "Heart Disease"),
      ("2", "30", "Male", "12345", "Diabetes"),
      ("3", "30", "Male", "12345", "Flu"),
      ("4", "45", "Female", "67890", "Heart Disease"),
      ("5", "45", "Female", "67890", "Cancer"),
      ("6", "45", "Female", "67890", "Flu"),
      ("7", "25", "Male", "11111", "Diabetes"),
      ("8", "25", "Male", "11111", "Flu"),
      ("9", "60", "Female", "22222", "Cancer")
    ).toDF("patient_id", "age", "gender", "zipcode", "disease")

    val params = PrivacyRiskParams(
      quasiIdentifiers = Seq("age", "gender", "zipcode"),
      sensitiveAttribute = Some("disease"),
      idColumn = Some("patient_id")
    )

    val result = PrivacyRiskAssessment.assess(
      patientData,
      params,
      kThreshold = 3,
      lThreshold = 2,
      tThreshold = 0.3
    )

    val report = PrivacyRiskAssessment.generateReport(result)
    println(report)

    // Example 2: Automatic Quasi-Identifier Detection
    println("\n" + "=" * 80)
    println("Example 2: Automatic Quasi-Identifier Detection")
    println("=" * 80)

    val employeeData = Seq(
      ("1", "30", "Male", "12345", "Engineering", "80000"),
      ("2", "25", "Female", "12346", "Marketing", "70000"),
      ("3", "30", "Male", "12347", "Engineering", "85000"),
      ("4", "45", "Female", "67890", "Sales", "90000")
    ).toDF("employee_id", "age", "gender", "zipcode", "department", "salary")

    val detectedQuasiIds = PrivacyRiskAssessment.detectQuasiIdentifiers(
      employeeData,
      excludeColumns = Seq("employee_id", "salary") // Exclude ID and sensitive data
    )

    println(s"Detected quasi-identifiers: ${detectedQuasiIds.mkString(", ")}")

    val autoParams = PrivacyRiskParams(
      quasiIdentifiers = detectedQuasiIds,
      sensitiveAttribute = Some("salary"),
      idColumn = Some("employee_id")
    )

    val autoResult = PrivacyRiskAssessment.assess(employeeData, autoParams)
    println("\nRisk Assessment with Auto-Detected Quasi-Identifiers:")
    println(s"Overall Risk Score: ${autoResult.overallRiskScore.toInt}/100")
    println("\nRecommendations:")
    autoResult.recommendations.zipWithIndex.foreach { case (rec, idx) =>
      println(s"${idx + 1}. $rec")
    }

    // Example 3: Comparing Datasets Before and After Anonymization
    println("\n" + "=" * 80)
    println("Example 3: Comparing Risk Before and After Anonymization")
    println("=" * 80)

    val rawData = Seq(
      ("1", "30", "Male", "12345"),
      ("2", "31", "Female", "12346"),
      ("3", "32", "Male", "12347"),
      ("4", "33", "Female", "12348")
    ).toDF("id", "age", "gender", "zipcode")

    // Apply generalization (simulated)
    val generalizedData = rawData
      .withColumn("age", ($"age" / 10).cast("int") * 10) // Age ranges: 30-39
      .withColumn("zipcode", $"zipcode".substr(1, 3)) // First 3 digits of zipcode

    val rawParams = PrivacyRiskParams(
      quasiIdentifiers = Seq("age", "gender", "zipcode"),
      idColumn = Some("id")
    )

    val rawRisk = PrivacyRiskAssessment.assess(rawData, rawParams)
    val generalizedRisk = PrivacyRiskAssessment.assess(generalizedData, rawParams)

    println(s"Raw Data Risk Score: ${rawRisk.overallRiskScore.toInt}/100")
    println(s"Generalized Data Risk Score: ${generalizedRisk.overallRiskScore.toInt}/100")
    println(s"Risk Reduction: ${(rawRisk.overallRiskScore - generalizedRisk.overallRiskScore).toInt} points")

    spark.stop()
  }
}
