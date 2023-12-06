package org.mitchelllisle.generaliser

import org.apache.spark.sql.DataFrame

class Generaliser(strategies: Seq[GeneralisationStrategy]) {
  /**
   * Applies a sequence of generalisation strategies to a DataFrame.
   *
   * This function takes a DataFrame and a sequence of generalisation strategies. Each strategy is applied
   * in the order they appear in the sequence. The result of one strategy is passed as input to the next.
   * This allows for compound generalisations where multiple strategies can be used in tandem to achieve
   * the desired level of data generalisation.
   *
   * For example, one might first want to map certain values in a column to broader categories, and then
   * further generalise by replacing them with range values.
   *
   * @param data       The DataFrame on which the generalisation strategies are to be applied.
   *                   The strategies are applied in the order they appear in the sequence.
   * @return A DataFrame that has been transformed by applying all the generalisation strategies.
   * @example
   * {{{
   * val data = Seq(1, 5, 13, 15).toDF("Numbers")
   * val rangeStrategy = RangeGeneralization("Numbers", 10)
   * val mapStrategy = MappingGeneralisation("Numbers", Map(1 -> "One", 5 -> "Five"))
   * val generaliser = new Generaliser(Seq(mapStrategy, rangeStrategy))
   * val result = generaliser.generalise(data)
   * }}}
   */
  def generalise(data: DataFrame): DataFrame = {
    strategies.foldLeft(data) { (currentData, strategy) => strategy.apply(currentData) }
  }
}
