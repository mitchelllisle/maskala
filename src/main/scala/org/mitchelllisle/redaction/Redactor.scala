package org.mitchelllisle.redaction

import org.apache.spark.sql.DataFrame

/** The Redactor class facilitates the redaction of data in a DataFrame using a sequence of redaction strategies. It
  * allows for applying multiple redaction strategies to a DataFrame, each modifying the data according to its own
  * rules.
  *
  * @param strategies
  *   A sequence of RedactionStrategy instances that define how data redaction should be performed.
  */
class Redactor(strategies: Seq[RedactionStrategy]) {

  /** Applies the sequence of redaction strategies to a DataFrame. Each strategy is applied in order, with the output of
    * one strategy becoming the input for the next.
    *
    * @param data
    *   The DataFrame to be redacted.
    * @return
    *   A DataFrame that has been modified by each of the provided redaction strategies in sequence.
    */
  def apply(data: DataFrame): DataFrame = {
    strategies.foldLeft(data) { (currentData, strategy) => strategy.apply(currentData) }
  }
}
