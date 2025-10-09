[![codecov](https://codecov.io/gh/mitchelllisle/maskala/graph/badge.svg?token=LCZ99996YR)](https://codecov.io/gh/mitchelllisle/maskala)

# Maskala

Maskala is a comprehensive privacy engineering toolkit built for Apache Spark. It provides enterprise-grade features for
analyzing, protecting, and managing sensitive data to ensure individuals in datasets are protected from re-identification
while maintaining compliance with privacy regulations like GDPR and CCPA.

> [!WARNING]
> Disclaimer: Anonymisation is hard.
    The data privacy and security techniques used in this project, such as K-Anonymity and data redaction, are intended to 
    assess and mitigate the risk of re-identification and _may_ provide you with a means to reduce the risk inherent in
    working with private data. However, **they will not provide** complete anonymisation and should not be seen as foolproof
    solutions. For some use cases you should seek more accepted means of anonymisation such as 
    [differential privacy](https://en.wikipedia.org/wiki/Differential_privacy), or through the best technique
    of all: Not collecting personal data to begin with.


## Features

### Privacy Risk Assessment
- **PrivacyRiskScorer**: Comprehensive risk scoring combining uniqueness and k-anonymity metrics
- **Risk Level Classification**: Automatic categorization (CRITICAL, HIGH, MEDIUM, LOW)
- **High-Risk Identification**: Pinpoint individuals requiring additional protection
- **Privacy Standards Validation**: Verify datasets meet organizational privacy thresholds

### Data Retention & Compliance
- **RetentionAnalyser**: Identify records exceeding retention periods for GDPR/CCPA compliance
- **Grace Period Support**: Configurable grace periods before deletion
- **Multi-Date Policies**: Smart retention based on multiple date columns
- **Audit Reports**: Detailed retention status summaries for compliance documentation

### Cryptographic Verification
- **Merkle Tree Proofs**: Tamper-evident audit trails for data integrity
- **Deletion Verification**: Cryptographic proof of record deletion
- **Combined Privacy Analysis**: Uniqueness assessment with retention proofs

### Anonymiser
The Anonymiser class is part of the Scala-based data anonymisation toolkit designed to apply various anonymisation 
strategies to data stored in Apache Spark DataFrames. This toolkit allows for the configuration-driven anonymisation of 
specific columns in a DataFrame, supporting strategies like masking, encryption, range generalisation, and more.

Let's step through an example. Imagine we have the following data (Note: This is an example Netflix dataset from the )

```csv
user_id,rating,date,movie,location
1815755,5,2004-07-20,Dinosaur Planet,Dominica
1426604,4,2005-09-01,Dinosaur Planet,Svalbard & Jan Mayen Islands
1535440,4,2005-08-18,Dinosaur Planet,Monaco
```

```scala
import org.apache.spark.sql.SparkSession
import org.mitchelllisle.Anonymiser

val spark = SparkSession.builder.appName("AnonymisationApp").getOrCreate()
val df = spark.read.load("your-data-source")

val anonymiser = new Anonymiser("path/to/your/config.yaml")
anonymiser(df)

```

```yaml
catalog: 'your_catalog'
schema: 'your_schema'
table: 'your_table'
anonymise:
  - column: 'columnName1'
    strategy: 'MaskingStrategy'
    parameters:
      mask: 'XXXX'
  - column: 'columnName2'
    strategy: 'RangeStrategy'
    parameters:
      rangeWidth: 10
      separator: '-'
  - column: 'columnName3'
    strategy: 'EncryptionStrategy'
    parameters:
      secret: 'your-secret-key'
analyse:
  - type: 'AnalysisType1'
    parameters:
      param1: 'value1'
      param2: 'value2'
```

## Getting Started
These methods are tools to aid in understanding and reducing re-identification risks and should be used as part of a
broader data protection strategy. Remember, no single method can ensure total data privacy and security.

### KAnonymity
K-Anonymity is a concept in data privacy that aims to ensure an individual's information cannot be distinguished from a
least k-1 others in a dataset. Essentially, it means that each individual's data is indistinguishable from at least k-1 
other individuals within the dataset. This is achieved by generalizing, suppressing, or altering specific identifiers 
(like names, addresses, or other personal details) until each person cannot be uniquely identified from a group of at 
least k individuals. K-Anonymity _might_ help mitigate the risk of re-identification in published data, making it useful 
in protecting personal information in datasets. However, it's important to note that while K-Anonymity can reduce the 
risk of identity disclosure, [it has been shown to be susceptible to re-identification attacks](https://course.ece.cmu.edu/~ece734/lectures/lecture-2018-10-08-deanonymization.pdf) 

#### 1. Assessing KAnonymity
You can assess if your dataset satisfies KAnonymity by using the `isKAnonymous` method:

```scala

import org.apache.spark.sql.SparkSession

val spark = SparkSession.builder().getOrCreate()

import spark.implicits._

// In the below DataFrame there are only two rows that match, meaning the other two don't satisfy K(2) Anonymity
val data = Seq(
  ("30", "Male"),
  ("30", "Male"),
  ("18", "Female"),
  ("45", "Female")
).toDF("Age", "Gender")

val kAnon = new KAnonymity(2)
val evaluated = kAnon.isKAnonymous(data) // returns false
```

#### 2. Filtering out rows that aren't KAnonymous
If you want a dataset that only contains the rows that meet KAnonymity, you can use the `removeLessThanKRows` method

```scala

import org.apache.spark.sql.SparkSession

val spark = SparkSession.builder().getOrCreate()

import spark.implicits._

val data = Seq(
  ("30", "Male"),
  ("30", "Male"),
  ("18", "Female"),
  ("45", "Female")
).toDF("Age", "Gender")

val kAnon = new KAnonymity(2)

val result = kAnon.removeLessThanKRows(data)
/* result would only contain the first two rows above:
("30", "Male"),
("30", "Male"),
* */
```

> Note: Now if you run `isKAnonymous(result)` it will return `true` since we've removed the rows that don't satisfy K(2).


###  ℓ-Diversity
ℓ-Diversity is an extension of the K-Anonymity principle in data privacy, designed to enhance the protection against 
certain types of attacks that K-Anonymity is susceptible to. While K-Anonymity ensures that each individual is 
indistinguishable from at least k-1 others in the dataset, ℓ-Diversity goes further by requiring that each group of 
indistinguishable individuals has at least 'l' distinct values for sensitive attributes. This concept addresses the 
limitation of K-Anonymity in scenarios where sensitive attributes within a group can be homogeneous, thereby still 
posing a risk of attribute disclosure. ℓ-Diversity _ensures diversity in sensitive information_, reducing the likelihood 
that an individual's sensitive attributes can be accurately inferred within an anonymized dataset. It's particularly 
useful in preventing attacks like homogeneity and background knowledge attacks, contributing to a more robust 
privacy-preserving data publication. However, similar to K-Anonymity, ℓ-Diversity is not a comprehensive solution. For
more information (including the limitations of l-diversity) 
I recommend reading [ℓ-Diversity: Privacy Beyond k-Anonymity](https://personal.utdallas.edu/~muratk/courses/privacy08f_files/ldiversity.pdf)

#### 1: Assessing ℓ-Diversity
You can assess if your dataset satisfies ℓ-Diversity by using the `isLDiverse` method:

```scala
import org.mitchelllisle.ldiversity.LDiversity
import org.apache.spark.sql.SparkSession

val spark = SparkSession.builder().getOrCreate()

import spark.implicits._

val data = Seq(
  ("A", "Male"),
  ("A", "Male"),
  ("B", "Female"),
  ("B", "Other")
).toDF("QuasiIdentifier", "SensitiveAttribute")

val lDiv = new LDiversity(2)
val evaluated = lDiv.isLDiverse(data) // returns false
```

#### 2. Filtering out rows that aren't ℓ-diverse
If you want a dataset that only contains the rows that meet ℓ-Diversity, you can use the `removeLessThanLRows` method

```scala
import org.mitchelllisle.ldiversity.LDiversity
import org.apache.spark.sql.SparkSession

val spark = SparkSession.builder().getOrCreate()

import spark.implicits._

val data = Seq(
  ("A", "Male"),
  ("A", "Male"),
  ("B", "Female"),
  ("B", "Other")
).toDF("QuasiIdentifier", "SensitiveAttribute")

val kAnon = new LDiversity(2)

val result = kAnon.removeLessThanKRows(data)
/* result would only contain the first two rows above:
("30", "Male"),
("30", "Male"),
* */
```

### Uniqueness Analyzer
The `UniquenessAnalyser` class in `org.mitchelllisle.reidentifiability` package provides methods to analyze the 
uniqueness of values within a DataFrame using Spark. Uniqueness is a proxy for re-identifiability, an important privacy 
engineering concept. This class helps evaluate re-identifiability risk metrics using data uniqueness as an indicator.

```scala

import org.apache.spark.sql.SparkSession

val spark = SparkSession.builder.getOrCreate()

val data = spark.read.option("header", "true").csv("src/test/resources/netflix-sample.csv")

val result = UniquenessAnalyser(table)
```

### Privacy Risk Scoring
The `PrivacyRiskScorer` provides comprehensive privacy risk assessment for individuals in your dataset. It combines 
multiple privacy metrics (uniqueness, k-anonymity) to generate an overall re-identification risk score.

```scala
import org.mitchelllisle.analysers.PrivacyRiskScorer

val spark = SparkSession.builder.getOrCreate()
val userData = spark.read.option("header", "true").csv("user-data.csv")

// Calculate risk scores for each individual
val riskScores = PrivacyRiskScorer.calculateRiskScores(
  data = userData,
  quasiIdentifiers = Seq("age", "gender", "zipcode"),
  idColumn = "user_id"
)

riskScores.show()
// Shows: individual_id, uniqueness_score, k_anonymity_score, overall_risk_score, risk_level, group_size

// Identify high-risk individuals who need additional protection
val highRisk = PrivacyRiskScorer.identifyHighRiskIndividuals(
  userData, 
  Seq("age", "gender", "zipcode"), 
  "user_id",
  riskThreshold = 0.5
)

// Generate a summary of risk distribution
val riskSummary = PrivacyRiskScorer.generateRiskSummary(
  userData,
  Seq("age", "gender", "zipcode"),
  "user_id"
)

// Check if dataset meets privacy standards
val isPrivacyCompliant = PrivacyRiskScorer.meetsPrivacyStandard(
  userData,
  Seq("age", "gender", "zipcode"),
  "user_id",
  maxAcceptableRisk = 0.3,
  maxHighRiskPercentage = 0.1
)
```

### Data Retention & Deletion Management
The `RetentionAnalyser` helps identify records that should be deleted based on retention policies, essential for GDPR, 
CCPA, and other privacy regulations.

```scala
import org.mitchelllisle.analysers.{RetentionAnalyser, RetentionPolicy}
import java.time.LocalDate

val spark = SparkSession.builder.getOrCreate()
val userData = spark.read.option("header", "true").csv("user-data.csv")

// Define retention policy: keep data for 365 days with 30-day grace period
val policy = RetentionPolicy(retentionDays = 365, gracePeriodDays = 30)

// Identify records for deletion
val retentionResult = RetentionAnalyser.identifyRecordsForDeletion(
  data = userData,
  idColumn = "user_id",
  dateColumn = "last_activity_date",
  policy = policy
)

println(s"Records for deletion: ${retentionResult.idsForDeletion}")
println(s"Records in grace period: ${retentionResult.idsInGracePeriod}")

// For multi-date retention (keeps record if ANY date is recent)
val multiDateResult = RetentionAnalyser.identifyRecordsForDeletionMultiDate(
  data = userData,
  idColumn = "user_id",
  dateColumns = Seq("last_login_date", "last_purchase_date", "account_created_date"),
  policy = policy
)

// Create detailed retention summary
val summary = RetentionAnalyser.createRetentionSummary(
  userData,
  "user_id",
  "last_activity_date",
  policy
)

summary.show()
// Shows: retention_status, unique_ids, total_records, oldest_date, newest_date
```

### Data Redaction
The Redactor class along with redaction strategies allows for flexible redaction of data in a DataFrame. You can apply 
multiple redaction strategies including masking, hashing and more.

```scala

import org.apache.spark.sql.SparkSession

val spark = SparkSession.builder.getOrCreate()

val redactor = new Redactor(Seq(
  MaskingStrategy("movie", "*****"),
  HashingStrategy("user_id"),
))

val data = spark.read.option("header", "true").csv("src/test/resources/netflix-sample.csv")
val redactedData = redactor(data)

```

## **Complete Privacy Engineering Workflow**

Maskala provides a comprehensive toolkit for privacy engineering. Here's how to use multiple features together:

### Step 1: Assess Privacy Risks

```scala
import org.mitchelllisle.analysers.PrivacyRiskScorer

// Calculate risk scores for your dataset
val riskScores = PrivacyRiskScorer.calculateRiskScores(
  data = userData,
  quasiIdentifiers = Seq("age", "gender", "zipcode"),
  idColumn = "user_id"
)

// Check if dataset meets privacy standards
val meetsStandard = PrivacyRiskScorer.meetsPrivacyStandard(
  userData,
  Seq("age", "gender", "zipcode"),
  "user_id",
  maxAcceptableRisk = 0.3
)

if (!meetsStandard) {
  println("⚠️ Dataset does not meet privacy standards - apply transformations")
}
```

### Step 2: Identify Records for Deletion

```scala
import org.mitchelllisle.analysers.{RetentionAnalyser, RetentionPolicy}

// Define retention policy
val policy = RetentionPolicy(retentionDays = 365, gracePeriodDays = 30)

// Find records to delete
val retentionResult = RetentionAnalyser.identifyRecordsForDeletion(
  userData,
  "user_id",
  "last_activity_date",
  policy
)

println(s"${retentionResult.totalRecordsForDeletion} records ready for deletion")
```

### Step 3: Verify Deletions with Cryptographic Proof

```scala
import org.mitchelllisle.analysers.MerkleTree

// Create proof before deletion
val beforeProof = MerkleTree.apply(userData, Seq("email", "age"), "user_id")

// Delete records
val afterData = userData.filter(!$"user_id".isin(retentionResult.idsForDeletion: _*))

// Generate deletion proof
val deletionProof = MerkleTree.verifyDeletion(
  beforeData = userData,
  afterData = afterData,
  deletedIds = retentionResult.idsForDeletion,
  columns = Seq("email", "age"),
  idColumn = "user_id"
)

// Validate
val isValid = MerkleTree.validateDeletionProof(
  deletionProof,
  expectedDeletions = retentionResult.totalRecordsForDeletion.toInt
)
```

### Step 4: Apply Privacy Transformations

```scala
// For high-risk individuals, apply stronger protections
val highRiskIds = PrivacyRiskScorer.identifyHighRiskIndividuals(
  userData,
  Seq("age", "gender", "zipcode"),
  "user_id",
  riskThreshold = 0.7
).select("individual_id").collect().map(_.getString(0))

// Apply targeted anonymization
val anonymiser = new Anonymiser("path/to/config.yaml")
val protectedData = anonymiser(userData)
```


## **Merkle Tree Data Retention & Audit Trails**

The `MerkleTree` provides cryptographic proof capabilities for data retention verification and tamper-evident audit trails. This is essential for regulatory compliance (GDPR, CCPA) and proving data deletion without revealing sensitive information.

### **Creating Data Integrity Proofs**

Generate cryptographic fingerprints of your datasets that change if any data is modified:
```scala
import org.mitchelllisle.analysers.MerkleTree

val spark = SparkSession.builder().getOrCreate()
val userData = spark.read.option("header", "true").csv("user-data.csv")

// Create tamper-evident proof of current data state
val dataProof = MerkleTree.apply(
  data = userData,
  columns = Seq("email", "age", "location"), // Columns to include in proof
  idColumn = "user_id"
)

println(s"Dataset fingerprint: ${dataProof.rootHash}")
println(s"Record count: ${dataProof.recordCount}")
```

## Verifying Data Deletions
Prove that specific records were actually deleted (not just hidden) with cryptographic evidence:

```scala
// Before deletion
val beforeData = userData
val beforeProof = MerkleTree.apply(beforeData, columns, "user_id")

// After user requests deletion
val afterData = userData.filter($"user_id" =!= "user123")

// Generate deletion proof
val deletionProof = MerkleTree.verifyDeletion(
  beforeData = beforeData,
  afterData = afterData, 
  deletedIds = Seq("user123"),
  columns = columns,
  idColumn = "user_id"
)

// Validate the proof
val isValidDeletion = MerkleTree.validateDeletionProof(
  deletionProof, 
  expectedDeletions = 1
)

if (isValidDeletion) {
  println("✓ Deletion cryptographically verified")
  // Proof is now validated - no JSON export needed
}
```

## Combined Privacy & Retention Analysis
Perform comprehensive analysis combining uniqueness assessment with retention proofs:

```scala
val (uniquenessAnalysis, retentionProof) = MerkleTree.combinedPrivacyAnalysis(
  dataFrame = userData,
  groupByColumns = Seq("age", "location"), // For uniqueness analysis
  userIdColumn = "user_id",
  retentionColumns = Seq("email", "age", "location"), // For retention proof
  k = 2048
)

// Analyze re-identification risks
uniquenessAnalysis.show()

// Cryptographic proof is available in retentionProof object
```

## Use Cases

**Privacy Risk Assessment & Management:**
- Automatically identify high-risk individuals in datasets before publication
- Calculate comprehensive privacy risk scores combining multiple metrics
- Ensure datasets meet organizational privacy standards before release
- Prioritize privacy-enhancing transformations for at-risk individuals

**Data Retention & Right to be Forgotten:**
- Identify customer records exceeding retention periods (GDPR, CCPA compliance)
- Provide grace periods before deletion to prevent accidental data loss
- Support multi-date retention policies (keep if ANY activity is recent)
- Generate audit-ready reports of retention status

**Cryptographic Deletion Verification:**
- Provide mathematical proof of data deletion to users and regulators
- Create tamper-evident audit trails for compliance requirements
- Verify deletions without revealing actual data contents
- Detect unauthorized changes to sensitive datasets

**Complete Privacy Engineering Workflows:**
- Assess → Identify → Delete → Verify → Protect pipeline
- Combine risk scoring, retention analysis, and cryptographic proofs
- Integrate privacy analysis into data engineering pipelines
- Automate compliance with privacy regulations


> [!NOTE]
> Merkle trees provide tamper-evident proofs but should be combined with access controls and encryption for complete data protection. The root hash acts as a "digital fingerprint" - any change to the data completely changes the hash, making unauthorized modifications immediately detectable.

### Dependencies

- Apache Spark 3.x

## Contributing
Pull requests are welcome. For major changes, please open an issue first to discuss what you would like to change.
