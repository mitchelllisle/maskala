package kanonymity

import org.apache.spark.sql.DataFrame

trait GeneralisationStrategy {
  def apply(data: DataFrame): DataFrame
}
