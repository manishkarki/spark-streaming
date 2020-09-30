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

  def streamingCount = {
    val lines = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 12345)
      .load()

    val lineCount = lines.selectExpr("count(*) as lineCount")

    //aggregates with distinct are not supported
    //otherwise spark will need to keep track of everything
    lineCount.writeStream
      .format("console")
      .outputMode("complete") // append and update not supported on aggregations without watermark
      .start()
      .awaitTermination()
  }

  streamingCount
}
