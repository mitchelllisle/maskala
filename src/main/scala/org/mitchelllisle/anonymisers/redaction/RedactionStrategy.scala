package org.mitchelllisle.anonymisers.redaction

import org.apache.spark.sql.DataFrame

/** A trait representing a redaction strategy for a DataFrame.
  *
  * Redaction is the process of removing or replacing sensitive information from data. This trait defines a method to
  * apply the redaction strategy on a given DataFrame.
  */
trait RedactionStrategy {
  def apply(data: DataFrame): DataFrame
}
