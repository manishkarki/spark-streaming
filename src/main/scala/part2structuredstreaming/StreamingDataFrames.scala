package part2structuredstreaming

import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * @author mkarki
  */
object StreamingDataFrames extends App {
  val spark = SparkSession.builder()
    .appName("first streams")
    .master("local[2]")
    .getOrCreate

  def readFromSocket() = {
    val lines: DataFrame = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("post", 12345)
      .load()

    val query = lines.writeStream
      .format("console")
      .outputMode(OutputMode.Append())
      .start()

    query.awaitTermination()
  }
}
