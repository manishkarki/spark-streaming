package part4integrations

import org.apache.spark.sql.SparkSession

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

    kafkaDF.writeStream
      .format("console")
      .outputMode("append")
      .start()
      .awaitTermination()
  }

  def main(args: Array[String]): Unit = {

  }

}
