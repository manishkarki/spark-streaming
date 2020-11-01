package part3lowlevel

import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{StreamingContext, Seconds}

/**
  * @author mkarki
  */
object DStreams {
  val spark = SparkSession.builder()
    .appName("DStreams")
    .master("local[2]")
    .getOrCreate()

  /*
    Spark streaming context = entry point to the DStreams API
    - needs the spark context
    - a duration = batch interval
   */
  val ssc = new StreamingContext(spark.sparkContext, Seconds(1))

  /*
    - define input sources by creating DStreams
    - define transformations on DStreams

   */
  def main(args: Array[String]): Unit = {

  }

}
