[![codecov](https://codecov.io/gh/mitchelllisle/maskala/graph/badge.svg?token=LCZ99996YR)](https://codecov.io/gh/mitchelllisle/maskala)

# Maskala

#### Features

- Compute the K-Anonymity of a given dataset and work with the data to achieve K-Anonymity
- Assess Uniqueness for a table to determine the risk of re-identifiability

## Getting Started

### KAnonymity

This library provides utility functions to assess and ensure K-Anonymity on datasets using Apache Spark.

```scala
import org.mitchelllisle.kanonymity.KAnonymity

val kAnonymity = new KAnonymity(k = 5)

val anonymizedData = kAnonymity.filter(data)
val isAnonymized = kAnonymity.evaluate(data)

val strategies = Seq(
  DateGeneralisation("dateColumn", QuarterYear),
  RangeGeneralisation("numberColumn", 10)
)
val generalisedData = kAnonymity.generalise(data, strategies)
```


### Uniqueness Analyzer
The `UniquenessAnalyser` class in `org.mitchelllisle.reidentifiability` package provides methods to analyze the 
uniqueness of values within a DataFrame using Spark. Uniqueness is a proxy for re-identifiability, an important privacy 
engineering concept. This class helps evaluate re-identifiability risk metrics using data uniqueness as an indicator.

```scala
import org.mitchelllisle.reidentifiability.UniquenessAnalyser
import org.apache.spark.sql.SparkSession

val spark = SparkSession.builder.master("local").appName("Uniqueness Analyser Example").getOrCreate()
val analyser = new UniquenessAnalyser(spark)

val data = spark.read.option("header", "true").csv("src/test/resources/netflix-sample.csv")
val table = analyser.getTable("netflix", "ratings", "customerId", Seq("rating"))

val result = analyser.apply(table)
```

### Data Redaction
The Redactor class along with redaction strategies allows for flexible redaction of data in a DataFrame. You can apply 
multiple redaction strategies including masking, hashing and more.

```scala
import org.mitchelllisle.redaction.{Redactor, MaskingStrategy, HashingStrategy, RemovalStrategy}
import org.apache.spark.sql.SparkSession

val spark = SparkSession.builder.master("local").appName("Data Redaction Example").getOrCreate()

val redactor = new Redactor(Seq(
  MaskingStrategy("movie", "*****"),
  HashingStrategy("user_id"),
))

val data = spark.read.option("header", "true").csv("src/test/resources/netflix-sample.csv")
val redactedData = redactor.apply(data)

```

### Dependencies

- Apache Spark 3.x

## Contributing
Pull requests are welcome. For major changes, please open an issue first to discuss what you would like to change.