package org.mitchelllisle.anonymisers.generalisation

import org.apache.spark.sql.DataFrame

/** Trait representing a generalisation strategy.
  *
  * A generalisation strategy defines how a given DataFrame is generalised.
  *
  * Example Usage:
  * {{{
  *   val strategy: GeneralisationStrategy = new MyGeneralisationStrategy()
  *   val originalData: DataFrame = ...
  *   val generalisedData: DataFrame = strategy.apply(originalData)
  * }}}
  */
trait GeneralisationStrategy {
  def apply(data: DataFrame): DataFrame
}
