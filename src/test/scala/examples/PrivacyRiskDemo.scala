package examples

import org.apache.spark.sql.SparkSession
import org.mitchelllisle.analysers.{PrivacyRiskAnalyser, PrivacyRiskFormatter}

/** Demonstration of the PrivacyRiskAnalyser functionality.
  *
  * This example shows how to use the comprehensive privacy risk assessment
  * to evaluate re-identification risks in a dataset.
  */
object PrivacyRiskDemo {

  def main(args: Array[String]): Unit = {
    // Initialize Spark
    val spark = SparkSession.builder()
      .appName("PrivacyRiskDemo")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    // Example 1: Basic K-Anonymity Assessment
    println("\n" + "=" * 70)
    println("Example 1: Basic Privacy Risk Assessment")
    println("=" * 70)

    val basicData = Seq(
      ("user1", "30-35", "Male", "USA", "Engineer"),
      ("user2", "30-35", "Male", "USA", "Engineer"),
      ("user3", "30-35", "Male", "USA", "Doctor"),
      ("user4", "25-30", "Female", "UK", "Teacher"),
      ("user5", "25-30", "Female", "UK", "Teacher"),
      ("user6", "25-30", "Female", "UK", "Nurse"),
      ("user7", "40-45", "Male", "Canada", "Manager"),
      ("user8", "35-40", "Female", "USA", "Developer")
    ).toDF("user_id", "age", "gender", "country", "occupation")

    val basicAssessment = PrivacyRiskAnalyser(
      data = basicData,
      k = 2,
      userIdColumn = "user_id",
      groupByColumns = Seq("age", "gender", "country")
    )

    PrivacyRiskFormatter.print(basicAssessment)

    // Example 2: Advanced Assessment with L-Diversity
    println("\n" + "=" * 70)
    println("Example 2: Privacy Risk Assessment with L-Diversity")
    println("=" * 70)

    val medicalData = Seq(
      ("patient1", "A", "Diabetes"),
      ("patient2", "A", "Hypertension"),
      ("patient3", "A", "Asthma"),
      ("patient4", "B", "Diabetes"),
      ("patient5", "B", "Cancer"),
      ("patient6", "C", "Flu"),
      ("patient7", "C", "Flu"),
      ("patient8", "C", "Flu")
    ).toDF("patient_id", "demographic_group", "diagnosis")

    val advancedAssessment = PrivacyRiskAnalyser(
      data = medicalData,
      k = 2,
      l = 2,
      userIdColumn = "patient_id",
      sensitiveColumn = "diagnosis",
      groupByColumns = Seq("demographic_group")
    )

    PrivacyRiskFormatter.print(advancedAssessment)

    // Example 3: High-Risk Dataset
    println("\n" + "=" * 70)
    println("Example 3: High-Risk Dataset Detection")
    println("=" * 70)

    val highRiskData = Seq(
      ("user1", "30", "Male", "12345", "IT"),
      ("user2", "25", "Female", "67890", "HR"),
      ("user3", "35", "Other", "11111", "Sales"),
      ("user4", "40", "Male", "22222", "Marketing"),
      ("user5", "28", "Female", "33333", "Finance")
    ).toDF("user_id", "age", "gender", "zipcode", "department")

    val highRiskAssessment = PrivacyRiskAnalyser(
      data = highRiskData,
      k = 2,
      userIdColumn = "user_id",
      groupByColumns = Seq("age", "gender", "zipcode")
    )

    PrivacyRiskFormatter.print(highRiskAssessment)

    // Example 4: Summary Output
    println("\n" + "=" * 70)
    println("Example 4: Quick Summary")
    println("=" * 70)

    println("\nBasic Dataset: " + PrivacyRiskFormatter.summary(basicAssessment))
    println("Medical Dataset: " + PrivacyRiskFormatter.summary(advancedAssessment))
    println("High-Risk Dataset: " + PrivacyRiskFormatter.summary(highRiskAssessment))

    println("\n" + "=" * 70)
    println("Demo Complete!")
    println("=" * 70)

    spark.stop()
  }
}
