package org.mitchelllisle.anonymisers

import org.apache.spark.sql.DataFrame

/** A trait defining the strategy for data anonymisation.
 *
 * This trait acts as a contract for implementing different anonymisation strategies.
 * Classes implementing this trait should provide the logic for how data anonymisation is to be applied on a DataFrame.
 *
 * The primary method `apply` should be implemented to take a DataFrame and anonymisation parameters,
 * and return a DataFrame where the anonymisation strategy has been applied.
 */
trait AnonymiserStrategy {

  /** Applies the anonymisation strategy to the provided DataFrame.
   *
   * This method should be implemented by concrete classes to specify how the anonymisation is to be performed.
   * It takes a DataFrame and anonymisation parameters, and returns a DataFrame with the anonymisation applied.
   *
   * @param data The DataFrame to be anonymised.
   * @param config The configuration parameters for anonymisation, encapsulated in an AnonymisationParams instance.
   * @return The anonymised DataFrame.
   */
  def apply(data: DataFrame, config: AnonymisationParams): DataFrame
}

/** A trait for encapsulating configuration parameters for anonymisation strategies.
 *
 * This trait serves as a base for defining various types of configuration parameters needed by different anonymisation strategies.
 * Concrete implementations of this trait will hold specific configuration details required by respective anonymisation strategies.
 */
trait AnonymisationParams
