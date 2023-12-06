package org.mitchelllisle.generaliser

import org.apache.spark.sql.{DataFrame, functions => F}

sealed trait DateLevel
case object YearOnly extends DateLevel
case object MonthYear extends DateLevel
case object QuarterYear extends DateLevel
case class CustomLevel(format: String) extends DateLevel

case class DateGeneralisation(columnName: String, level: DateLevel) extends GeneralisationStrategy {
  override def apply(data: DataFrame): DataFrame = {
    level match {
      case YearOnly =>
        data.withColumn(columnName, F.date_format(F.col(columnName), "yyyy"))
      case MonthYear =>
        data.withColumn(columnName, F.date_format(F.col(columnName), "yyyy-MM"))
      case QuarterYear =>
        val quarter = F.quarter(F.col(columnName))
        val year = F.date_format(F.col(columnName), "yyyy")
        data.withColumn(columnName, F.concat_ws("-", quarter, year))
      case CustomLevel(format) =>
        data.withColumn(columnName, F.date_format(F.col(columnName), format))
    }
  }
}
