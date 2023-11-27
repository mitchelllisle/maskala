[![codecov](https://codecov.io/gh/mitchelllisle/maskala/graph/badge.svg?token=LCZ99996YR)](https://codecov.io/gh/mitchelllisle/maskala)

# Maskala

#### Features

- Compute the K-Anonymity of a given dataset and work with the data to achieve K-Anonymity
- Assess Uniqueness for a table to determine the risk of re-identifiability

## Getting Started

### KAnonymity

This library provides utility functions to assess and ensure K-Anonymity on datasets using Apache Spark.

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

### Dependencies

- Apache Spark 3.x

## Contributing
Pull requests are welcome. For major changes, please open an issue first to discuss what you would like to change.