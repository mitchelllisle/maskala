package org.mitchelllisle.anonymisers

import org.apache.spark.sql.{DataFrame, functions => F}

case class MappingParams(mapping: String) extends AnonymisationParams

/** A generalisation strategy that applies a mapping to a specific column in a DataFrame.
  *
  * @param columnName
  *   The name of the column to apply the mapping to.
  */
case class MappingStrategy(columnName: String) extends AnonymiserStrategy {
  private def mappingStringToMap(mapping: String): Map[String, String] = {
    val keyValuePairs = mapping.split(",").map(_.trim.split("="))
    keyValuePairs.collect { case Array(key, value) => key -> value}.toMap
  }

  override def apply(data: DataFrame, params: AnonymisationParams): DataFrame = {
    params match {
      case mp: MappingParams =>
        val mappingValues = mappingStringToMap(mp.mapping)
        val mappingUdf = F.udf((input: String) => mappingValues.getOrElse(input, input))
        data.withColumn(columnName, mappingUdf(F.col(columnName)))
    }
  }
}
