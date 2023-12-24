package org.mitchelllisle.anonymisers

import org.apache.spark.sql.{DataFrame, functions => F}

case class MappingParams( mapping: Map[String, String]) extends AnonymisationParams

/** A generalisation strategy that applies a mapping to a specific column in a DataFrame.
  *
  * @param columnName
  *   The name of the column to apply the mapping to.
  */
case class MappingStrategy(columnName: String) extends AnonymiserStrategy {

  override def apply(data: DataFrame, params: AnonymisationParams): DataFrame = {
    params match {
      case mp: MappingParams =>
        val mappingUdf = F.udf((input: String) => mp.mapping.getOrElse(input, input))
        data.withColumn(columnName, mappingUdf(F.col(columnName)))
    }
  }
}
