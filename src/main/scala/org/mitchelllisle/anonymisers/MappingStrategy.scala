//package org.mitchelllisle.anonymisers
//
//import org.apache.spark.sql.{DataFrame, functions => F}
//
///** A generalisation strategy that applies a mapping to a specific column in a DataFrame.
//  *
//  * @param columnName
//  *   The name of the column to apply the mapping to.
//  * @param mapping
//  *   A map of input values to their corresponding output values.
//  */
//case class MappingStrategy(columnName: String, mapping: Map[String, String]) extends AnonymiserStrategy {
//
//  private val mappingUdf = F.udf((input: String) => mapping.getOrElse(input, input))
//
//  override def apply(data: DataFrame): DataFrame = {
//    data.withColumn(columnName, mappingUdf(F.col(columnName)))
//  }
//}
