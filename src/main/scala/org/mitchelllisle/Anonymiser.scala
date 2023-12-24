package org.mitchelllisle

import io.circe.{Decoder, Json}
import io.circe.generic.auto._
import io.circe.parser.decode
import io.circe.yaml.parser

import scala.io.Source
import org.apache.spark.sql.DataFrame
import org.mitchelllisle.anonymisers._

case class AnonymisationConfig(
    catalog: String,
    schema: String,
    table: String,
    anonymise: List[ColumnAnonymisation],
    analyse: List[AnalysisType]
)

case class ColumnAnonymisation(column: String, strategy: String, parameters: Option[Map[String, String]])

case class AnalysisType(`type`: String)

class ConfigError(message: String, cause: Throwable = null) extends Exception(message, cause)

class Anonymiser(configFilePath: String) {

  private val config: AnonymisationConfig = readConfig(configFilePath)

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

  private def mapToCaseClass[T](map: Map[String, String])(implicit decoder: Decoder[T]): T = {
    val jsonString = Json.obj(map.mapValues(Json.fromString).toSeq: _*).noSpaces
    decode[T](jsonString) match {
      case Left(_)      => throw new ConfigError(s"unable to parse $jsonString")
      case Right(value) => value
    }
  }

  private def getParams[T](parameters: Option[Map[String, String]])(implicit decoder: Decoder[T]): T = {
    mapToCaseClass[T](parameters.getOrElse(Map.empty))
  }

  def apply(data: DataFrame): DataFrame = {
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
}
