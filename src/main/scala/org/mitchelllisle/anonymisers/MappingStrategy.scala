package org.mitchelllisle.anonymisers

import org.apache.spark.sql.{DataFrame, functions => F}

/** Represents parameters for mapping-based anonymisation.
 *
 * @param mapping A string representing a set of key-value pairs for mapping.
 *                Each pair is separated by a comma, and the key and value within a pair are separated by an equals sign.
 *                For example, "original1=anonymised1,original2=anonymised2".
 */
case class MappingParams(mapping: String) extends AnonymisationParams

/** Implements a generalisation strategy for data anonymisation by applying a mapping to a specific column in a DataFrame.
 *
 * This strategy takes a column name and replaces its values based on a provided mapping. The mapping is defined in `MappingParams`.
 * Values in the specified column that are found in the mapping keys are replaced with the corresponding mapping values.
 * Values not found in the mapping are left unchanged.
 *
 * @param columnName The name of the column in the DataFrame to which the mapping will be applied.
 */
case class MappingStrategy(columnName: String) extends AnonymiserStrategy {

  /** Converts a mapping string to a Map.
   *
   * The method splits the input string by commas to get key-value pairs,
   * then further splits each pair by the equals sign to separate keys and values.
   * It finally collects these into a Map.
   *
   * @param mapping The mapping string to convert.
   * @return A Map representing the key-value pairs from the input string.
   */
  private def mappingStringToMap(mapping: String): Map[String, String] = {
    val keyValuePairs = mapping.split(",").map(_.trim.split("="))
    keyValuePairs.collect { case Array(key, value) => key -> value}.toMap
  }

  /** Applies the mapping strategy to a DataFrame.
   *
   * This method applies the mapping to the specified column in the DataFrame.
   * It uses a UDF to map each value in the column based on the provided `MappingParams`.
   * If a value in the column matches a key in the mapping, it is replaced with the corresponding value from the mapping.
   * Otherwise, the original value is retained.
   *
   * @param data The DataFrame to which the mapping will be applied.
   * @param params The parameters defining the mapping. Must be an instance of `MappingParams`.
   * @return A DataFrame with the mapping applied to the specified column.
   * @throws IllegalArgumentException if `params` is not an instance of `MappingParams`.
   */
  override def apply(data: DataFrame, params: AnonymisationParams): DataFrame = {
    params match {
      case mp: MappingParams =>
        val mappingValues = mappingStringToMap(mp.mapping)
        val mappingUdf = F.udf((input: String) => mappingValues.getOrElse(input, input))
        data.withColumn(columnName, mappingUdf(F.col(columnName)))
      case _ =>
        throw new IllegalArgumentException("MappingStrategy requires MappingParams")
    }
  }
}
