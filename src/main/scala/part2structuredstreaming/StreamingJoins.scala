package part2structuredstreaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, from_json}

/**
  * @author mkarki
  */
object StreamingJoins extends App {
  val spark = SparkSession.builder()
    .appName("streaming joins")
    .master("local[2]")
    .getOrCreate()

  val guitarPlayers = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/guitarPlayers")

  val guitars = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/guitars")

  val bands = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/bands")

  val joinCondition = guitarPlayers.col("band") === bands.col("id")

  // join of static DFs
  val guitaristsBands = guitarPlayers.join(bands, joinCondition)
  val bandsSchema = bands.schema

  def joinStreamWithStatic() = {
    val streamedBandsDF = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 12345)
      .load // this contains a single column "value" of type String
      .select(from_json(col("value"), bandsSchema).as("band"))
      .selectExpr("band.id as id", "band.name as name", "band.hometown as hometown", "band.year as year")

    // join happens PER BATCH
    val streamedBandsGuitaristsDF = streamedBandsDF.join(guitarPlayers,
      guitarPlayers.col("band") === streamedBandsDF.col("id"))

    streamedBandsGuitaristsDF.writeStream
      .format("console")
      .outputMode("append")
      .start()
      .awaitTermination()
  }

  joinStreamWithStatic()

}
