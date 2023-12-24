package org.mitchelllisle.anonymisers

import org.apache.spark.sql.{DataFrame, functions => F}

case class DateParams(format: String) extends AnonymisationParams

/** A class representing a generalisation strategy for dates.
  *
  * @param column
  *   The name of the column to apply the generalisation strategy to.
  */
case class DateStrategy(column: String) extends AnonymiserStrategy {
  override def apply(data: DataFrame, params: AnonymisationParams): DataFrame = {
    params match {
      case dp: DateParams => data.withColumn(column, F.date_format(F.col(column), dp.format))
    }
  }
}
