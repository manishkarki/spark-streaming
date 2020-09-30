package part2structuredstreaming

import org.apache.spark.sql.SparkSession

/**
  * @author mkarki
  */
object StreamingAggregations extends App {
  val spark = SparkSession.builder()
    .appName("streaming aggregations")
    .master("local[2]")
    .getOrCreate
}
