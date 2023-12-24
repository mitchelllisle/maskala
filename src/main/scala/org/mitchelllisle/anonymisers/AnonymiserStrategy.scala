package org.mitchelllisle.anonymisers

import org.apache.spark.sql.DataFrame

trait AnonymiserStrategy {
  def apply(data: DataFrame, config: AnonymisationParams): DataFrame
}

trait AnonymisationParams
