package part2structuredstreaming

import org.apache.spark.sql.SparkSession

/**
  * @author mkarki
  */
object StreamingDataFrames extends App {
  val spark = SparkSession.builder()
    .appName("first streams")
    .master("local[2]")
    .getOrCreate
}
