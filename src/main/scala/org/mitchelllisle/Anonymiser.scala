package org.mitchelllisle

import io.circe.{Decoder, Json}
import io.circe.generic.auto._
import io.circe.parser.decode
import io.circe.yaml.parser

import scala.io.Source
import org.apache.spark.sql.DataFrame
import org.mitchelllisle.anonymisers._
import org.mitchelllisle.analysers._


/** Represents the configuration for data anonymisation and analysis.
 *
 * @param catalog The database catalog.
 * @param schema The database schema.
 * @param table The table to be anonymised.
 * @param anonymise A list of column-specific anonymisation configurations.
 * @param analyse A list of analysis types to be performed on the data.
 */
case class AnonymisationConfig(
    catalog: String,
    schema: String,
    table: String,
    anonymise: List[ColumnAnonymisation],
    analyse: List[AnalysisType]
)

/** Represents the configuration for anonymising a specific column.
 *
 * @param column The name of the column to be anonymised.
 * @param strategy The anonymisation strategy to be applied to the column.
 * @param parameters Optional parameters specific to the chosen anonymisation strategy.
 */
case class ColumnAnonymisation(column: String, strategy: String, parameters: Option[Map[String, String]])

/** Represents the type of analysis to be performed on the data.
 *
 * @param type The type of analysis.
 * @param parameters Optional parameters specific to the chosen type of analysis.
 */
case class AnalysisType(`type`: String, parameters: Option[Map[String, String]])

/** Exception to be thrown when there is an error in the configuration process.
 *
 * @param message The error message.
 * @param cause The underlying cause of the error (if any).
 */
class ConfigError(message: String, cause: Throwable = null) extends Exception(message, cause)

/** Manages the anonymisation process based on a provided configuration file.
 *
 * @param configFilePath The path to the YAML configuration file.
 */
class Anonymiser(configFilePath: String) {

  private val config: AnonymisationConfig = readConfig(configFilePath)

  /** Reads and parses the configuration from a YAML file.
   *
   * @param path The file path of the YAML configuration.
   * @return The parsed AnonymisationConfig object.
   * @throws ConfigError if there is an error in parsing or decoding the YAML file.
   */
  def readConfig(path: String): AnonymisationConfig = {
    val fileContent = Source.fromFile(path)
    val parsedYaml = parser.parse(fileContent.mkString)
    fileContent.close()

    parsedYaml match {
      case Left(parseError) => throw new ConfigError(s"Failed to parse YAML: $parseError")
      case Right(json) =>
        json.as[AnonymisationConfig] match {
          case Left(decodingError) => throw new ConfigError(s"Failed to decode: $decodingError")
          case Right(config)       => config
        }
    }
  }

  /** Converts a map of parameters to a case class instance using implicit decoding.
   *
   * @param map The map of parameters.
   * @tparam T The type of the case class to which the map will be converted.
   * @return An instance of the case class T.
   * @throws ConfigError if unable to parse the map into the case class.
   */
  private def mapToCaseClass[T](map: Map[String, String])(implicit decoder: Decoder[T]): T = {
    val jsonString = Json.obj(map.mapValues(Json.fromString).toSeq: _*).noSpaces
    decode[T](jsonString) match {
      case Left(_)      => throw new ConfigError(s"unable to parse $jsonString")
      case Right(value) => value
    }
  }

  /** Retrieves and converts anonymisation parameters from an optional map.
   *
   * @param parameters An optional map of parameters.
   * @tparam T The type of the case class representing the anonymisation parameters.
   * @return An instance of the case class T.
   */
  private def getParams[T](parameters: Option[Map[String, String]])(implicit decoder: Decoder[T]): T = {
    mapToCaseClass[T](parameters.getOrElse(Map.empty))
  }

  /** Executes the anonymisation strategies on the provided DataFrame.
   *
   * @param data The DataFrame to be anonymised.
   * @return The anonymised DataFrame.
   */
  def runAnonymisers(data: DataFrame): DataFrame = {
    config.anonymise.foldLeft(data) { (currentData, columnConfig) =>
      columnConfig.strategy match {
        case "MaskingStrategy" =>
          val params = getParams[MaskingParams](columnConfig.parameters)
          MaskingStrategy(columnConfig.column).apply(currentData, params)
        case "HashingStrategy" =>
          HashingStrategy(columnConfig.column).apply(currentData)
        case "RangeStrategy" =>
          val params = getParams[RangeParams](columnConfig.parameters)
          RangeStrategy(columnConfig.column).apply(currentData, params)
        case "DateStrategy" =>
          val params = getParams[DateParams](columnConfig.parameters)
          DateStrategy(columnConfig.column).apply(currentData, params)
        case "MappingStrategy" =>
          val params = getParams[MappingParams](columnConfig.parameters)
          MappingStrategy(columnConfig.column).apply(currentData, params)
        case "EncryptionStrategy" =>
          val params = getParams[EncryptionParams](columnConfig.parameters)
          EncryptionStrategy(columnConfig.column).apply(currentData, params)
        case _ => throw new ConfigError(s"${columnConfig.strategy} is not a recognised anonymisation strategy")
      }
    }
  }

  def runAnalysers(data: DataFrame): Unit = {
    config.analyse.foldLeft(data) { (_, analysisConfig) =>
      analysisConfig.`type` match {
        case "uniqueness" =>
          val params = getParams[UniquenessParams](analysisConfig.parameters)
          val output = UniquenessAnalyser(data, data.columns, userIdColumn = params.idColumn)
          output.show()
          output
        case "k-anonymity" =>
          val params = getParams[KAnonymityParams](analysisConfig.parameters)
          val output = new KAnonymity(params.k).apply(data, params.idColumn)
          output.show()
          output
        case "l-diversity" =>
          val params = getParams[LDiversityParams](analysisConfig.parameters)
          val output = new LDiversity(params.l).apply(data, params.idColumn)
          output.show()
          output
        case _ => throw new ConfigError(s"${analysisConfig.`type`} is not a recognised analyser")
      }
    }
  }

  /** Applies the anonymisation and analysis configuration to a DataFrame.
   *
   * @param data The DataFrame to be processed.
   */
  def apply(data: DataFrame): Unit = {
    if (config.anonymise.isEmpty) {
      throw new RuntimeException(
        "No anonymisation strategies provided. You must supply a config with at least one anonymisation strategy."
      )
    } else {
      val anonymised = runAnonymisers(data)
      if (config.analyse.nonEmpty) {
        runAnalysers(anonymised)
      }
    }
  }
}
