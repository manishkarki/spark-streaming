package part2structuredstreaming

import common.{Car, carsSchema}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, from_json}

/**
  * @author mkarki
  */
object StreamingDatasets extends App {
  val spark = SparkSession.builder()
    .appName("streaming datasets")
    .master("local[2]")
    .getOrCreate()

  // includes encoders for DF -> DS transformations
  import spark.implicits._
  def readCars() = {
    spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 12345)
      .load() // DF with single string column value
      .select(from_json(col("value"), carsSchema).as("car")) // composite column (struct)
      .selectExpr("car.*")
      .as[Car] // encoders required which is fulfilled by implicits import
    // or can be passed implicitly by creating a custom one as Encoders.product[Car]

  }

  def showCarNames() = {
    val carsDS = readCars()

    // collection transformation
    val carNamesAlt = carsDS.map(_.Name)

    carNamesAlt.writeStream
      .format("console")
      .outputMode("append")
      .start()
      .awaitTermination()
  }

  /*
      Exercises:
      1) how many powerful cars we have in the dataset(hp > 140)
      2) AVERAGE HP for the entire DS
        (USE the complete output mode)
      3) count the cars by their origin
   */

  // 1)
  def processPowerfulCars() = {
    val carsDS = readCars()
    val powerfulCars = carsDS.select(col("Name"), col("HorsePower"))
      .filter(col("HorsePower") > 140)


    powerfulCars.writeStream
      .format("console")
      .outputMode("append")
      .start()
      .awaitTermination()
  }

  processPowerfulCars()
}
