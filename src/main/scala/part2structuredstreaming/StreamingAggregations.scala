package part2structuredstreaming

import org.apache.spark.sql.{Column, SparkSession, functions}
import org.apache.spark.sql.functions.{col, stddev, sum}

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

  def numericalAggregations() = {
    val lines = getLines()
    val numbers = lines.select(col("value").cast("integer").as("numValue"))
    val sumDF = numbers.select(functions.sum(col("numValue")))
      .as("agg_so_far")

    sumDF.writeStream
      .format("console")
      .outputMode("complete")
      .start()
      .awaitTermination()
  }

  def numericalAggregationsWithParam(aggFunction: Column => Column) = {
    val lines = getLines()
    val numbers = lines.select(col("value").cast("integer").as("numValue"))
    val sumDF = numbers.select(aggFunction(col("numValue")))
      .as("agg_so_far")

    sumDF.writeStream
      .format("console")
      .outputMode("complete")
      .start()
      .awaitTermination()
  }

  def getLines() = {
    spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 12345)
      .load()
  }

  // counting the occurrences of name
  def groupNames() = {
    val lines = getLines()

    val names = lines.select(col("value"))
      .groupBy(col("value"))
      .count()

    names.writeStream
      .format("console")
      .outputMode("complete")
      .start()
      .awaitTermination()
  }

//  streamingCount
//  numericalAggregationsWithParam(sum)
  groupNames()
}
