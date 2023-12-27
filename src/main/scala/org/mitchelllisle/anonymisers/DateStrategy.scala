package org.mitchelllisle.anonymisers

import org.apache.spark.sql.{DataFrame, functions => F}

/** Represents parameters for date anonymisation.
 *
 * @param format A string representing the date format to be used for anonymisation.
 *               This format follows the Java SimpleDateFormat conventions.
 *               For example, "yyyy-MM-dd" for a format of year-month-day.
 */
case class DateParams(format: String) extends AnonymisationParams

/** Implements a generalisation strategy for anonymising date data in a DataFrame.
 *
 * This strategy takes a column containing date information and transforms its values
 * to a specified format. The desired format is defined in `DateParams`.
 * It is useful for reducing the precision of dates to a more general form,
 * thus contributing to data anonymisation.
 *
 * @param column The name of the column in the DataFrame containing date values to be anonymised.
 */
case class DateStrategy(column: String) extends AnonymiserStrategy {

  /** Applies the date anonymisation strategy to a DataFrame.
   *
   * This method uses the `date_format` function from Spark SQL to convert date values in the specified column
   * to the format defined in `DateParams`. If the `params` argument is not an instance of `DateParams`,
   * the method will not perform any transformation.
   *
   * @param data The DataFrame to be anonymised.
   * @param params The parameters defining the date format. Must be an instance of `DateParams`.
   * @return A DataFrame with the date anonymisation applied to the specified column.
   * @throws IllegalArgumentException if `params` is not an instance of `DateParams`.
   */
  override def apply(data: DataFrame, params: AnonymisationParams): DataFrame = {
    params match {
      case dp: DateParams => data.withColumn(column, F.date_format(F.col(column), dp.format))
      case _ =>
        throw new IllegalArgumentException("DateStrategy requires DateParams")
    }
  }
}
