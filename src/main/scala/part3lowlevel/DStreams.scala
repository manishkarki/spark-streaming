package part3lowlevel

import java.io.{File, FileWriter}
import java.sql.Date
import java.text.SimpleDateFormat

import common.Stock
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * @author mkarki
  */
object DStreams {
  val spark = SparkSession
    .builder()
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
    - call an action on DStreams
    - start ALL computations with ssc.start()
      - no more computations can be added
    - await termination or stop the computation
      - you cannot restart a computation
   */

  def readFromSocket() = {
    val socketStream: DStream[String] = ssc.socketTextStream("localhost", 12345)

    // transformation = lazy
    val wordsStream: DStream[String] =
      socketStream.flatMap(line => line.split(" "))

    // action
    wordsStream.print()

    ssc.start()
    ssc.awaitTermination()
  }

  def createNewFiles() = {
    new Thread(() => {
      Thread.sleep(5000)
      val path = "src/main/resources/data/stocks"
      val dir = new File(path)
      val nFiles = dir.listFiles().length
      val newFile = new File(s"$path/newStocks$nFiles.csv")

      val writer = new FileWriter(newFile)
      writer.write(
        """
          |AAPL,Jun 1 2001,11.62
          |AAPL,Jul 1 2001,9.4
          |AAPL,Aug 1 2001,9.27
          |AAPL,Sep 1 2001,7.76
          |AAPL,Oct 1 2001,8.78
          |AAPL,Nov 1 2001,10.65
          |AAPL,Dec 1 2001,10.95
          |AAPL,Jan 1 2002,12.36
          |AAPL,Feb 1 2002,10.85
          |AAPL,Mar 1 2002,11.84
          |AAPL,Apr 1 2002,12.14
          |AAPL,May 1 2002,11.65
          |AAPL,Jun 1 2002,8.86
          |AAPL,Jul 1 2002,7.63
          |AAPL,Aug 1 2002,7.38
          |AAPL,Sep 1 2002,7.25
          |AAPL,Oct 1 2002,8.03
          |AAPL,Nov 1 2002,7.75
          |AAPL,Dec 1 2002,7.16
          |AAPL,Jan 1 2003,7.18
          |AAPL,Feb 1 2003,7.51
          |AAPL,Mar 1 2003,7.07
          |AAPL,Apr 1 2003,7.11
          |AAPL,May 1 2003,8.98
          |""".stripMargin.trim)
    })
  }

  def readFromFile() = {
    createNewFiles() // operates on another thread

    val stocksFilePath = "src/main/resources/data/stocks"
    /*
      ssc.textFileStream monitors a directory for new files
     */
    val textStream: DStream[String] = ssc.textFileStream(stocksFilePath)

    val dateFormat = new SimpleDateFormat("MMM d yyyy")
    // transformations
    val stockStream: DStream[Stock] = textStream.map(line => {
      val tokens = line.split(",")
      val company = tokens(0)
      val date = new Date(dateFormat.parse(tokens(1)).getTime)
      val price = tokens(2).toDouble

      Stock(company, date, price)

    })

    // action
    stockStream.print()

    ssc.start()
    ssc.awaitTermination()
  }

  def main(args: Array[String]): Unit = {
//    readFromSocket()
    readFromFile()
  }

}
