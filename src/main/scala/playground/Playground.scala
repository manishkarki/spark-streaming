package playground

import org.apache.spark.sql.SparkSession

/**
  * @author mkarki
  */
object Playground extends App {
  val spark = SparkSession.builder()
    .appName("plaground")
    .config("spark.master", "local")
    .getOrCreate()

  val sc = spark.sparkContext
}
