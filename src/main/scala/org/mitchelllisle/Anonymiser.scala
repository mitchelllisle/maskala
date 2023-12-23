package org.mitchelllisle

import io.circe.generic.auto._
import io.circe.yaml.parser
import scala.io.Source

case class Config(
    catalog: String,
    schema: String,
    table: String,
    anonymise: List[AnonymiseStrategy],
    analyse: List[AnalyseType]
)

case class AnonymiseStrategy(column: String, strategy: String)
case class AnalyseType(`type`: String)

class Anonymiser(configFilePath: String) {

  def readConfig(): Config = {
    val fileContent = Source.fromFile(configFilePath)
    val parsedYaml = parser.parse(fileContent.mkString)
    fileContent.close()

    parsedYaml match {
      case Left(parseError) => throw new RuntimeException(s"Failed to parse YAML: ${parseError}")
      case Right(json) =>
        json.as[Config] match {
          case Left(decodingError) => throw new RuntimeException(s"Failed to decode: ${decodingError}")
          case Right(config)       => config
        }
    }
  }

  def apply(): Unit = {
    val config = readConfig()
  }
}
