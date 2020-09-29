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
    // reading a DF
    val lines: DataFrame = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 12345)
      .load()

    //consuming a DF
    val query = lines.writeStream
      .format("console")
      .outputMode(OutputMode.Append())
      .start()

    query.awaitTermination()
  }

  readFromSocket()
}
