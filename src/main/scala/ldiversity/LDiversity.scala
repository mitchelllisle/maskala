package ldiversity

import kanonymity.KAnonymity
import org.apache.spark.sql.{DataFrame, functions => F}

class LDiversity(l: Int, k: Int = 1) extends KAnonymity(k) {
  /**
   * Checks if a DataFrame meets l-diversity requirements.
   *
   * @param data            DataFrame containing the dataset.
   * @param sensitiveColumn The sensitive column that needs to have at least l diverse values.
   * @param l               The minimum number of distinct sensitive values that a group should have.
   * @return A boolean indicating if the DataFrame meets l-diversity.
   */
  def evaluate(data: DataFrame, sensitiveColumn: String): Boolean = {
    val groupColumns = data.columns.filter(col => col != sensitiveColumn)

    val groupedData = data
      .groupBy(groupColumns.map(data(_)): _*)
      .agg(F.countDistinct(sensitiveColumn).as("distinctCount"))

    groupedData.filter(F.col("distinctCount") < l).count() == 0
  }
}
