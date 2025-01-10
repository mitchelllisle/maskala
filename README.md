[![codecov](https://codecov.io/gh/mitchelllisle/maskala/graph/badge.svg?token=LCZ99996YR)](https://codecov.io/gh/mitchelllisle/maskala)

# Maskala

Maskala is a privacy engineering toolkit that works with Apache Spark. It aims to provide a number of features for
analysing, masking, generalising and filtering data to help ensure the identity of individuals in a dataset are protected
from re-identification.

> [!WARNING]
> ⚠️ Disclaimer: Anonymisation is hard.
    The data privacy and security techniques used in this project, such as K-Anonymity and data redaction, are intended to 
    assess and mitigate the risk of re-identification and _may_ provide you with a means to reduce the risk inherent in
    working with private data. However, **they will not provide** complete anonymisation and should not be seen as foolproof
    solutions. For some use cases you should seek more accepted means of anonymisation such as 
    [differential privacy](https://en.wikipedia.org/wiki/Differential_privacy), or through the best technique
    of all: Not collecting personal data to begin with.


## Features

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

### Dependencies

- Apache Spark 3.x

## Contributing
Pull requests are welcome. For major changes, please open an issue first to discuss what you would like to change.
