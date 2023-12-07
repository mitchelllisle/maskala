package org.mitchelllisle.generalisation

import org.apache.spark.sql.{DataFrame, functions => F}

case class MappingGeneralisation(columnName: String, mapping: Map[String, String]) extends GeneralisationStrategy {

  private val mappingUdf = F.udf((input: String) => mapping.getOrElse(input, input))

  override def apply(data: DataFrame): DataFrame = {
    data.withColumn(columnName, mappingUdf(F.col(columnName)))
  }
}
