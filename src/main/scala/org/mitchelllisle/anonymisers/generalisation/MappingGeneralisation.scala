package org.mitchelllisle.anonymisers.generalisation

import org.apache.spark.sql.{DataFrame, functions => F}

/** A generalisation strategy that applies a mapping to a specific column in a DataFrame.
  *
  * @param columnName
  *   The name of the column to apply the mapping to.
  * @param mapping
  *   A map of input values to their corresponding output values.
  */
case class MappingGeneralisation(columnName: String, mapping: Map[String, String]) extends GeneralisationStrategy {

  private val mappingUdf = F.udf((input: String) => mapping.getOrElse(input, input))

  override def apply(data: DataFrame): DataFrame = {
    data.withColumn(columnName, mappingUdf(F.col(columnName)))
  }
}
