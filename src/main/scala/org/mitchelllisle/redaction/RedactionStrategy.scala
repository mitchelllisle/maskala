package org.mitchelllisle.redaction

import org.apache.spark.sql.DataFrame

trait RedactionStrategy {
  def apply(data: DataFrame): DataFrame
}
