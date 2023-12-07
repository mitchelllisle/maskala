package org.mitchelllisle.redaction

import org.apache.spark.sql.DataFrame

class Redactor(strategies: Seq[RedactionStrategy]) {
  def apply(data: DataFrame): DataFrame = {
    strategies.foldLeft(data) { (currentData, strategy) => strategy.apply(currentData) }
  }
}
