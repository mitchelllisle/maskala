import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.count

object KAnonymity {
  def isKAnonymous(df: DataFrame, k: Int): Boolean = {
    val groupedDf = df.groupBy(df.columns.map(df(_)): _*).agg(count("*").as("count"))
    val minCount = groupedDf.agg(Map("count" -> "min")).collect()(0)(0).asInstanceOf[Long]
    minCount >= k
  }
}