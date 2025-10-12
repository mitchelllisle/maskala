# Data Retention Detection - Quick Start Guide

This guide shows how to use the RetentionDetector feature to identify and handle data that exceeds retention policies.

## Basic Usage

### 1. Define a Retention Policy

```scala
import org.mitchelllisle.analysers.RetentionPolicy

val gdprPolicy = RetentionPolicy(
  name = "GDPR_DEFAULT",
  retentionDays = 365,
  description = Some("GDPR 1-year retention policy")
)
```

### 2. Detect Violations

```scala
import org.mitchelllisle.analysers.RetentionDetector

val violations = RetentionDetector.detectViolations(
  data = customerData,
  idColumn = "customer_id",
  timestampColumn = "last_activity_date",
  policy = gdprPolicy
)

violations.show()
```

### 3. Get Summary Statistics

```scala
val (violations, summary) = RetentionDetector.detectViolationsWithSummary(
  customerData,
  "customer_id",
  "last_activity_date",
  gdprPolicy
)

println(summary)
// Output:
// Retention Policy: GDPR_DEFAULT
// Total Records: 10000
// Violations Found: 1234
// Compliance Rate: 87.66%
```

## Advanced Usage

### Multiple Policies

```scala
val policies = Seq(
  RetentionPolicy("30_DAY", 30),
  RetentionPolicy("90_DAY", 90),
  RetentionPolicy("1_YEAR", 365)
)

val allViolations = RetentionDetector.detectMultiplePolicies(
  customerData,
  "customer_id",
  "last_activity_date",
  policies
)
```

### Action Planning

```scala
val actionPlan = RetentionDetector.generateActionPlan(
  violations,
  actionThresholdDays = 30
)

// Shows: customerId, daysPastRetention, recommendedAction, priority
actionPlan.show()
```

### Violation Analysis

```scala
val violationsByRange = RetentionDetector.groupViolationsByRange(violations)

// Shows violations grouped by time ranges:
// 0-30 days, 31-90 days, 91-180 days, 181-365 days, Over 1 year
violationsByRange.show()
```

## Integration with Audit Trails

```scala
import org.mitchelllisle.analysers.MerkleTree

// Before cleanup
val beforeProof = MerkleTree.apply(
  customerData,
  Seq("customer_id", "email", "last_activity_date"),
  "customer_id"
)

// Perform cleanup based on violations
val cleanedData = customerData.join(
  violations.select("customerId"),
  customerData("customer_id") === violations("customerId"),
  "left_anti"
)

// After cleanup
val afterProof = MerkleTree.apply(
  cleanedData,
  Seq("customer_id", "email", "last_activity_date"),
  "customer_id"
)

// Verify deletion
val deletionProof = MerkleTree.verifyDeletion(
  beforeData = customerData,
  afterData = cleanedData,
  deletedIds = violations.select("customerId").collect().map(_.getString(0)),
  columns = Seq("customer_id", "email", "last_activity_date"),
  idColumn = "customer_id"
)
```

## Common Use Cases

### 1. GDPR Compliance Check

```scala
val gdprPolicy = RetentionPolicy("GDPR", 365)
val (violations, summary) = RetentionDetector.detectViolationsWithSummary(
  userData, "user_id", "last_login", gdprPolicy
)

if (summary.complianceRate < 95.0) {
  println(s"WARNING: Compliance rate is ${summary.complianceRate}%")
}
```

### 2. Marketing Data Cleanup

```scala
val marketingPolicy = RetentionPolicy("MARKETING", 180)
val violations = RetentionDetector.apply(
  marketingData,
  "contact_id",
  "consent_date",
  marketingPolicy
)

val toDelete = violations.filter($"daysPastRetention" > 30)
```

### 3. Scheduled Audit Report

```scala
val policies = Seq(
  RetentionPolicy("ACTIVE_USERS", 90),
  RetentionPolicy("INACTIVE_USERS", 365),
  RetentionPolicy("DELETED_USERS", 30)
)

policies.foreach { policy =>
  val (violations, summary) = RetentionDetector.detectViolationsWithSummary(
    userData, "user_id", "last_activity", policy
  )
  
  println(summary)
  
  // Save report
  violations
    .withColumn("report_date", current_timestamp())
    .write
    .mode("append")
    .parquet(s"reports/retention-violations/${policy.name}")
}
```

## Column Type Support

The RetentionDetector automatically handles different column types:

```scala
// Timestamp columns
val tsData = df.select($"id", $"timestamp".cast(TimestampType))
RetentionDetector.apply(tsData, "id", "timestamp", policy)

// Date columns
val dateData = df.select($"id", $"date".cast(DateType))
RetentionDetector.apply(dateData, "id", "date", policy)

// String columns (automatically converted)
val stringData = df.select($"id", $"date_string")
RetentionDetector.apply(stringData, "id", "date_string", policy)
```

## Best Practices

1. **Start with analysis**: Use `detectViolationsWithSummary()` to understand the scope before taking action
2. **Use action plans**: Generate action plans with thresholds to prioritize critical violations
3. **Create audit trails**: Always use MerkleTree before and after cleanup for compliance proof
4. **Test with recent date**: Pass a custom reference date for testing: `referenceDate = Some(LocalDate.of(2024, 1, 1))`
5. **Monitor compliance**: Track compliance rates over time to identify trends
6. **Automate workflows**: Integrate with scheduled Spark jobs for continuous monitoring

## Performance Tips

- Use partitioning on timestamp columns for faster filtering
- Cache the violations DataFrame if using it multiple times
- Use `limit()` when testing with large datasets
- Consider using Spark's broadcast join for small violation sets

## Troubleshooting

### Issue: No violations detected when expected

```scala
// Check the data types
customerData.printSchema()

// Verify the timestamp column values
customerData.select("last_activity_date").show()

// Check the reference date (defaults to current date)
val violations = RetentionDetector.detectViolations(
  customerData, "id", "timestamp", policy,
  referenceDate = Some(LocalDate.now())
)
```

### Issue: Column type error

Ensure your timestamp column is one of:
- TimestampType
- DateType  
- StringType (will be auto-converted)

Convert if needed:
```scala
val converted = df.withColumn("ts", to_timestamp($"date_string"))
```

## Next Steps

For a complete end-to-end example with deletion, anonymization, and audit trails, see:
`src/main/scala/org/mitchelllisle/examples/RetentionPipelineExample.scala`
