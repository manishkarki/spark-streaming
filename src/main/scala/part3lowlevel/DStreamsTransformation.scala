package part3lowlevel

import java.sql.Date
import java.time.{LocalDate, Period}

import common.Person
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * @author mkarki
  */
object DStreamsTransformation {
  val spark = SparkSession.builder()
    .appName("DStreams transformation")
    .master("local[2]")
    .getOrCreate()

  val ssc = new StreamingContext(spark.sparkContext, Seconds(1))

  // read the file in streaming netcat
  def readPeople() = ssc.socketTextStream("localhost", 9999)
    .map(line => {
      val tokens = line.split(":")
      Person(
        tokens(0).toInt, // id
        tokens(1), // first name
        tokens(2), // middle name
        tokens(3), // last name
        tokens(4), // gender
        Date.valueOf(tokens(5)), //birth
        tokens(6), // ssn/uuid
        tokens(7).toInt // salary
      )
    })

  // map, flatMap, filter
  def peopleAges() = readPeople().map(person => {
    val age = Period.between(person.birthDate.toLocalDate, LocalDate.now()).getYears
    (s"${person.firstName} ${person.lastName}", age)
  })

  def main(args: Array[String]): Unit = {
    val stream = readPeople()
    stream.print()
    ssc.start()
    ssc.awaitTermination()
  }

}
