[![codecov](https://codecov.io/gh/mitchelllisle/maskala/graph/badge.svg?token=LCZ99996YR)](https://codecov.io/gh/mitchelllisle/maskala)

# Maskala

## Features

- Compute the K-Anonymity of a given dataset and work with the data to achieve K-Anonymity
- Assess Uniqueness for a table to determine the risk of re-identifiability

## Getting Started

### KAnonymity

This library provides utility functions to assess and ensure K-Anonymity on datasets using Apache Spark.

#### 1. Assessing KAnonymity
You can assess if your dataset satisfies KAnonymity by using the `isKAnonymous` method:
```scala
import org.mitchelllisle.kanonymity.KAnonymity
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
If you want a dataset that only contains the rows that meet KAnonymity, you can use the 1

```scala
import org.mitchelllisle.kanonymity.KAnonymity
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

### Uniqueness Analyzer
The `UniquenessAnalyser` class in `org.mitchelllisle.reidentifiability` package provides methods to analyze the 
uniqueness of values within a DataFrame using Spark. Uniqueness is a proxy for re-identifiability, an important privacy 
engineering concept. This class helps evaluate re-identifiability risk metrics using data uniqueness as an indicator.

```scala
import org.mitchelllisle.reidentifiability.UniquenessAnalyser
import org.apache.spark.sql.SparkSession

val spark = SparkSession.builder.getOrCreate()
val analyser = new UniquenessAnalyser(spark)

val data = spark.read.option("header", "true").csv("src/test/resources/netflix-sample.csv")

val result = analyser(table)
```

### Data Redaction
The Redactor class along with redaction strategies allows for flexible redaction of data in a DataFrame. You can apply 
multiple redaction strategies including masking, hashing and more.

```scala
import org.mitchelllisle.redaction.{Redactor, MaskingStrategy, HashingStrategy}
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
