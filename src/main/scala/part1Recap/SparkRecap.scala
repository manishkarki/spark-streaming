package part1Recap

import org.apache.spark.sql.SparkSession

/**
  * @author mkarki
  */
object SparkRecap extends App {

  //entry point
  val spark = SparkSession.builder()
    .appName("Spark recap")
    .config("spark.master", "local")
    .getOrCreate()

  // read a DF
  val carsDF = spark.read
    .format("json")
    .load("src/main/resources/data/cars/cars.json")

  // another way
  val carsDF2 = spark.read
    .json("src/main/resources/data/cars/cars.json")
}
