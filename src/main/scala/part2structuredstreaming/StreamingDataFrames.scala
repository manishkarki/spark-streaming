package part2structuredstreaming

import common.stocksSchema
import org.apache.spark.sql.functions.{col, length}
import org.apache.spark.sql.streaming.{OutputMode, Trigger}
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.concurrent._
import scala.concurrent.duration.DurationInt

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

    // check if DF is streaming or not
    println(shortLines.isStreaming)
    //consuming a DF
    val query = lines.writeStream
      .format("console")
      .outputMode(OutputMode.Append())
      .start()

    //wait for the stream to finish
    query.awaitTermination()
  }

  def readFromFiles() = {
    val stocksDF: DataFrame = spark.readStream
      .format("csv")
      .option("header", "false")
      .option("dateFormat", "MMM d yyyy")
      .schema(stocksSchema)
      .load("src/main/resources/data/stocks")

    stocksDF.writeStream
      .format("console")
      .outputMode("append") // default outputMode
      .start()
      .awaitTermination()
  }

  def demoTriggers() = {
    val lines: DataFrame = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 12345)
      .load()

    // write the lines at certain trigger
    lines.writeStream
      .format("console")
      .outputMode("append")
      .trigger(Trigger.ProcessingTime(2.seconds)) // every 2 seconds run the query
      .start()
      .awaitTermination()
  }

//  readFromSocket()
//  readFromFiles()
  demoTriggers()
}
