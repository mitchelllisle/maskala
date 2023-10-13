package kanonymity

import kanonymity.generalisation.GeneralisationStrategy
import org.apache.spark.sql.{DataFrame, functions => F}

import java.security.MessageDigest

class KAnonymity(k: Int) {

  private val hashUdf = F.udf[String, String](hashRow)

  /**
   * Generates a SHA-256 hash for a given string.
   *
   * @param s The input string to be hashed.
   * @return A string representing the SHA-256 hash of the input.
   */
  private def hashRow(s: String): String =
    MessageDigest.getInstance("SHA-256")
      .digest(s.getBytes("UTF-8"))
      .map("%02x".format(_))
      .mkString

  /**
   * Adds a hash column to the dataframe by considering specific columns.
   *
   * @param df      The dataframe to which the hash column will be added.
   * @param columns The columns which will be used to generate the hash.
   * @return A new dataframe with an additional "row_hash" column.
   */
  private def getHashedData(df: DataFrame, columns: Array[String]): DataFrame =
    df.withColumn("row_hash", hashUdf(F.concat_ws("|", columns.map(df(_)): _*)))

  /**
   * Computes frequency counts of unique rows in the dataframe while considering specific columns.
   *
   * @param data          The dataframe whose rows' frequencies will be computed.
   * @param ignoreColumns Columns to ignore while computing the row frequencies.
   * @return A dataframe with a "row_hash" and a "count" column.
   */
  private def getRowFrequencyCounts(data: DataFrame, ignoreColumns: Seq[String] = Seq.empty): DataFrame = {
    val columnsToConsider = data.columns.filterNot(ignoreColumns.contains)
    getHashedData(data, columnsToConsider)
      .groupBy(F.col("row_hash"))
      .agg(F.count("*").as("count"))
  }

  /**
   * Filters the dataframe to only include rows whose frequencies meet a minimum threshold (k).
   *
   * @param data          The dataframe to be filtered.
   * @param ignoreColumns Columns to ignore while determining row uniqueness.
   * @return A dataframe with rows that meet the minimum frequency threshold.
   */
  def filter(data: DataFrame, ignoreColumns: Seq[String] = Seq.empty): DataFrame = {
    val columnsToConsider = data.columns.filterNot(ignoreColumns.contains)
    val countsDf = getRowFrequencyCounts(data, ignoreColumns)
    val hashedData = getHashedData(data, columnsToConsider)

    hashedData
      .join(countsDf, "row_hash")
      .filter(F.col("count") >= k)
      .drop("count", "row_hash")
  }


  /**
   * Determines if a dataframe satisfies the conditions of K-Anonymity.
   *
   * @param data          The dataframe to be checked for K-Anonymity.
   * @param ignoreColumns Columns to ignore while determining row uniqueness.
   * @return `true` if the dataframe satisfies K-Anonymity,
   *         `false` if not,
   */
  def evaluate(data: DataFrame, ignoreColumns: Seq[String] = Seq.empty): Boolean = {
    val countsDf = getRowFrequencyCounts(data, ignoreColumns)
    val minCount = countsDf
      .agg(F.min("count").as("min"))
      .first()
      .getAs[Long]("min")
    minCount >= k
  }

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
   * @param data The DataFrame on which the generalisation strategies are to be applied.
   * @param strategies The sequence of generalisation strategies to be applied on the data.
   *                   The strategies are applied in the order they appear in the sequence.
   *
   * @return A DataFrame that has been transformed by applying all the generalisation strategies.
   *
   * @example
   * {{{
   * val data = Seq(1, 5, 13, 15).toDF("Numbers")
   * val rangeStrategy = RangeGeneralization("Numbers", 10)
   * val mapStrategy = MappingGeneralisation("Numbers", Map(1 -> "One", 5 -> "Five"))
   * val result = generalise(data, Seq(mapStrategy, rangeStrategy))
   * }}}
   */
  def generalise(data: DataFrame, strategies: Seq[GeneralisationStrategy]): DataFrame = {
    strategies.foldLeft(data) {(currentData, strategy) => strategy.apply(currentData)}
  }
}
