package part2structuredstreaming

import org.apache.spark.sql.functions.{col, length}
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

    // transformation
    val shortLines = lines
      .filter(length(col("value")) <= 5)

    //consuming a DF
    val query = lines.writeStream
      .format("console")
      .outputMode(OutputMode.Append())
      .start()

    //wait for the stream to finish
    query.awaitTermination()
  }

  readFromSocket()
}
