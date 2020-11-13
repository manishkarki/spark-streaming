package part4integrations

import common.carsSchema
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, expr, struct, to_json}

/**
  * @author mkarki
  */
object KafkaIntegration {

  val spark = SparkSession.builder()
    .appName("Kafka integration")
    .master("local[2]")
    .getOrCreate()

  def readFromKafka() = {
    val kafkaDF = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "sparkstreaming")
      .load()

    kafkaDF.select(col("topic"), expr("cast(value as string) as actualValue"))
      .writeStream
      .format("console")
      .outputMode("append")
      .start()
      .awaitTermination()
  }

  def writeToKafka() = {
    val carsKafkaDF = carsDF.selectExpr("upper(Name) as key", "name as value")

    carsKafkaDF.writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("topic", "sparkstreaming")
      .option("checkpointLocation", "checkpoints")
      .start()
      .awaitTermination()
  }

  val carsDF = spark.readStream
    .schema(carsSchema)
    .json("src/main/resources/data/cars")

  /*
      Exercise: write the whole cars data structures to the kafka as JSON.
      USE struct columns and the to_json function.
   */
  def writeWholeCarsDSToKafka() = {
    val carsJsonKafkaDF = carsDF.select(
      col("Name").as("key"),
      to_json(struct(col("Name"), col("Horsepower"), col("Origin"))).cast("String").as("value")
    )

    carsJsonKafkaDF.writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("topic", "sparkstreaming")
      .option("checkpointLocation", "checkpoints")
      .start()
      .awaitTermination()
  }

  def main(args: Array[String]): Unit = {
    writeWholeCarsDSToKafka()
  }

}
