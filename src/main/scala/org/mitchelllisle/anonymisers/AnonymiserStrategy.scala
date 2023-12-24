package org.mitchelllisle.anonymisers

import org.apache.spark.sql.DataFrame

class MissingParameter(message: String, cause: Throwable = null) extends Exception(message, cause)

trait AnonymiserStrategy {
  def apply(data: DataFrame, config: AnonymisationParams): DataFrame
}

trait AnonymisationParams
