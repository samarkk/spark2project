package com.skk.training.structuredstreaming

import java.sql.Timestamp
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Row
import scala.reflect.api.materializeTypeTag
import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.streaming.ProcessingTime
import org.apache.spark.sql.streaming.StreamingQueryListener
import org.apache.spark.sql.streaming.StreamingQueryListener.{ QueryProgressEvent, QueryStartedEvent, QueryTerminatedEvent }

object StreamingWordCount extends App {
  // first run plain word count - uncomment val wordCount = ... and val query = wordCount.
  // map("sco " +) - since we have output mode complete we will get in each batch counts for
  // everything that would have been punched in to the console till that time

  // then comment out wordcounts and query and uncomment windowed word counts and
  // windowed query and run

  // then sequentially we can go on, uncomment watermarkedwindow counts and query
  // and finally the batch table operations where we can join with existing data
  // in this case a demo wimptable and kind of add on metadata info to the streaming
  // operations
  val spark = SparkSession.builder()
    .appName("StreamingWordCount")
    .master("local[*]")
    .config("hive.metastore.uris", "thrift://quickstart.cloudera:9083")
    .config("spark.sql.shuffle.partitions", "2")
    .enableHiveSupport()
    .getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")

  import spark.implicits._
  //  addStreamingQueryListeners(spark, false)

  val lines = spark.readStream
    .format("socket")
    .option("host", "localhost")
    .option("port", 9999)
    .load()
    .withColumn("timestamp", current_timestamp())

  val words = lines.as[(String, Timestamp)]
    .flatMap(line => line._1.split(" ").map(
      word => (word, line._2)))
    .toDF("word", "timestamp")
  //
  //  val wordCount = words
  //    .withWatermark("timestamp", "5 seconds")
  //    .groupBy($"word")
  //    .count()

  //  val windowedWordCounts = words.
  //    groupBy(
  //      window($"timestamp", "4 seconds", "2 seconds"),
  //      $"word").count()
  //  windowedWordCounts.printSchema()

  val waterMarkedWindowCount = words
    .withWatermark("timestamp", "1 minutes")
    .groupBy(
      window($"timestamp", "1 minutes", "1 minutes"),
      $"word").count.toDF("wdw", "word", "counts")
  waterMarkedWindowCount.printSchema()

  //  val query = wordCount.map("sco " + _).writeStream
  //    .format("console")
  //    .outputMode("complete")
  //    //    .trigger(ProcessingTime(1000))
  //    .option("truncate", false)
  //    .start()
  //  //
  //  query.awaitTermination()

  //  val windowQuery =
  //    windowedWordCounts.writeStream
  //      //    windowedWordCounts.map(x => ((
  //      //      x.getAs[Row]("window").getAs[Timestamp](0),
  //      //      x.getAs[String](1), x.getAs[Long](2)))).writeStream
  //      .format("console")
  //      .outputMode("update")
  //      .option("truncate", false)
  //      //    .trigger(ProcessingTime(1000))
  //      .start()
  //      .awaitTermination()
  // enter a series of lines in the console and one can verify the sliding window operation
  // we will have a four second window sliding every two seconds

  // we can see watermarking operations also
  // the data is in the wccheck table in default hive db
  // we have a watermark interval 5 minutes and window of 5 minutes sliding every minute
  // so data punched in at 1715 will be available at 1726 as 1726 - (1715 +5) = 6 > 5
  // the watermarking duration
  //  waterMarkedWindowCount.writeStream
  //    .format("parquet")
  //    .outputMode("append")
  //    .option("truncate", false)
  //    .option("path", "hdfs://localhost:8020/user/cloudera/waterwrdc")
  //    .option("checkpointLocation", "file:///home/cloudera/watercheck")
  //    //    .trigger(ProcessingTime("10 seconds"))
  //    .start()
  //    .awaitTermination()

  val batchTbl = spark.read.table("wimptbl")
  //  //  batchTbl.show()
  //  println("words schema")
  //  words.printSchema()
  //  println("words left outer join batchtbl schema")
  //  words.join(batchTbl, Seq("word"), "left_outer").printSchema()
  //
  words.join(batchTbl, Seq("word"), "left_outer")
    .na.fill(0, Seq("freq"))
    .groupBy("word", "freq")
    //    .groupBy(window($"timestamp", "10 seconds", "5 seconds"), $"word", $"freq")
    .count()
    .withColumn("wtdfreq", $"freq" * $"count")
    .writeStream
    .format("console")
    .outputMode("complete")
    .start
    .awaitTermination()
}
