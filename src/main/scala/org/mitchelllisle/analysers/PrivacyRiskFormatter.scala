package org.mitchelllisle.analysers

/** Utility object for formatting and displaying privacy risk assessment results.
  */
object PrivacyRiskFormatter {

  /** Formats a privacy risk assessment into a human-readable string report.
    *
    * @param assessment The PrivacyRiskAssessment to format
    * @return A formatted string containing the full assessment report
    */
  def format(assessment: PrivacyRiskAssessment): String = {
    val sb = new StringBuilder

    sb.append("\n" + "=" * 60 + "\n")
    sb.append(" " * 15 + "PRIVACY RISK ASSESSMENT\n")
    sb.append("=" * 60 + "\n\n")

    // Overall Risk Summary
    sb.append("OVERALL RISK:\n")
    sb.append(s"  Risk Level: ${formatRiskLevel(assessment.overallRiskLevel)}\n")
    sb.append(f"  Risk Score: ${assessment.overallRiskScore}%.3f (0.0 = highest risk, 1.0 = lowest risk)\n\n")

    // Individual Metric Scores
    sb.append("METRIC SCORES:\n")
    sb.append(f"  K-Anonymity:  ${assessment.kAnonymityScore}%.3f\n")
    assessment.lDiversityScore.foreach { score =>
      sb.append(f"  L-Diversity:  $score%.3f\n")
    }
    sb.append(f"  Uniqueness:   ${assessment.uniquenessScore}%.3f\n\n")

    // Detailed Metrics
    sb.append("DETAILED METRICS:\n")

    // K-Anonymity Metrics
    val kMetrics = assessment.metrics("kAnonymity").asInstanceOf[Map[String, Any]]
    sb.append("  K-Anonymity:\n")
    sb.append(s"    Is K-Anonymous: ${kMetrics("isKAnonymous")}\n")
    sb.append(s"    Total Groups: ${kMetrics("totalGroups")}\n")
    sb.append(s"    Compliant Groups: ${kMetrics("compliantGroups")}\n")
    sb.append(s"    Non-compliant Groups: ${kMetrics("nonCompliantGroups")}\n")
    sb.append(s"    Min Group Size: ${kMetrics("minGroupSize")}\n")

    // L-Diversity Metrics (if available)
    assessment.metrics.get("lDiversity").foreach { lMetricsAny =>
      val lMetrics = lMetricsAny.asInstanceOf[Map[String, Any]]
      sb.append("  L-Diversity:\n")
      sb.append(s"    Is L-Diverse: ${lMetrics("isLDiverse")}\n")
      sb.append(s"    Total Groups: ${lMetrics("totalGroups")}\n")
      sb.append(s"    Compliant Groups: ${lMetrics("compliantGroups")}\n")
      sb.append(s"    Non-compliant Groups: ${lMetrics("nonCompliantGroups")}\n")
      sb.append(s"    Min Distinct Count: ${lMetrics("minDistinctCount")}\n")
    }

    // Uniqueness Metrics
    val uMetrics = assessment.metrics("uniqueness").asInstanceOf[Map[String, Any]]
    sb.append("  Uniqueness:\n")
    sb.append(f"    Unique Records Ratio: ${uMetrics("uniqueRatio").asInstanceOf[Double]}%.3f\n")

    // Recommendations
    sb.append("\n")
    sb.append("RECOMMENDATIONS:\n")
    assessment.recommendations.zipWithIndex.foreach { case (rec, idx) =>
      sb.append(s"  ${idx + 1}. $rec\n")
    }

    sb.append("\n" + "=" * 60 + "\n")

    sb.toString()
  }

  /** Formats the risk level with appropriate visual indicators.
    *
    * @param level The risk level string
    * @return Formatted risk level with visual indicator
    */
  private def formatRiskLevel(level: String): String = {
    level match {
      case "LOW" => s"$level ✓ (Good privacy protection)"
      case "MEDIUM" => s"$level ⚠ (Acceptable with caution)"
      case "HIGH" => s"$level ⚠⚠ (Significant privacy concerns)"
      case "CRITICAL" => s"$level ✗✗ (Severe privacy risks)"
      case _ => level
    }
  }

  /** Prints a formatted privacy risk assessment to the console.
    *
    * @param assessment The PrivacyRiskAssessment to print
    */
  def print(assessment: PrivacyRiskAssessment): Unit = {
    println(format(assessment))
  }

  /** Creates a summary string with just the key findings.
    *
    * @param assessment The PrivacyRiskAssessment to summarize
    * @return A brief summary string
    */
  def summary(assessment: PrivacyRiskAssessment): String = {
    val sb = new StringBuilder
    sb.append(s"Risk: ${assessment.overallRiskLevel} ")
    sb.append(f"(Score: ${assessment.overallRiskScore}%.2f) | ")
    sb.append(f"K-Anon: ${assessment.kAnonymityScore}%.2f | ")
    assessment.lDiversityScore.foreach(score => sb.append(f"L-Div: $score%.2f | "))
    sb.append(f"Unique: ${assessment.uniquenessScore}%.2f")
    sb.toString()
  }
}
