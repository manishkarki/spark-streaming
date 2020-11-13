package part4integrations

import common.carsSchema
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, expr}

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
    val carsDF = spark.readStream
      .schema(carsSchema)
      .json("src/main/resources/data/cars")

    val carsKafkaDF = carsDF.selectExpr("upper(Name) as key", "name as value")

    carsKafkaDF.writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("topic", "sparkstreaming")
      .option("checkpointLocation", "checkpoints")
      .start()
      .awaitTermination()
  }

  def main(args: Array[String]): Unit = {
    writeToKafka()
  }

}
