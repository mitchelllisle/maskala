package org.mitchelllisle.anonymisers.generalisation

import org.apache.spark.sql.{DataFrame, functions => F}

/** The `DateLevel` trait represents different levels of precision for date values. It is sealed which means all
  * implementations must be declared in this file.
  */
sealed trait DateLevel

/** Represents a date level of only year.
  *
  * Use this class to indicate that a date should be represented at the year level only. This can be useful when working
  * with dates where only the year value is available or needed.
  *
  * Example usage:
  * {{{
  *   val date: DateLevel = YearOnly
  * }}}
  */
case object YearOnly extends DateLevel

/** A singleton object representing a month and year combination.
  *
  * This class represents a specific month and year combination, without the specific day or time. It is a singleton
  * object, meaning that there can only be one instance of MonthYear in the application.
  */
case object MonthYear extends DateLevel

/** Represents a quarter of a year.
  *
  * The QuarterYear class is a case object that extends the DateLevel trait. It represents a quarter of a year in a
  * calendar. QuarterYear provides functionality to work with quarters, such as getting the current quarter, checking if
  * a specific quarter is valid, or calculating the next quarter.
  *
  * Quarter years are represented as integers in the range of 1 to 4, where each quarter corresponds to a specific
  * three-month period: 1 - January to March 2 - April to June 3 - July to September 4 - October to December
  *
  * QuarterYear is designed to be used as a singleton object, so there is only one instance of this class available in
  * the application.
  *
  * Usage:
  *   - Get the current quarter: val currentQuarter = QuarterYear.currentQuarter
  *   - Check if a quarter is valid: val isValid = QuarterYear.isValidQuarter(2)
  *   - Calculate the next quarter: val nextQuarter = QuarterYear.nextQuarter(3)
  *
  * Note that this class does not support creating new instances as it is implemented as a case object.
  */
case object QuarterYear extends DateLevel

/** Represents a custom log level based on the current date and time.
  *
  * @param format
  *   The format string used to represent the custom log level. It can include any valid format characters specified by
  *   `java.time.format.DateTimeFormatter`. Examples of format strings include "HH:mm:ss" or "yyyy-MM-dd HH:mm:ss.SSS".
  *   The format string will be used to format the current date and time.
  * @see
  *   java.time.format.DateTimeFormatter
  * @see
  *   java.time.LocalDateTime
  * @see
  *   java.util.logging.Level
  */
case class CustomLevel(format: String) extends DateLevel

/** A class representing a generalisation strategy for dates.
  *
  * @param columnName
  *   The name of the column to apply the generalisation strategy to.
  * @param level
  *   The level of generalisation to apply.
  */
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
