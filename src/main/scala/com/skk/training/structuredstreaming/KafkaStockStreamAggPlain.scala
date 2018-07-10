package com.skk.training.structuredstreaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming._
import org.apache.spark.sql.functions._
import java.sql.Timestamp
import scala.reflect.api.materializeTypeTag

object KafkaStockStreamAggPlain extends App {
  /*
   * empty the checkpoint directory
   * if need be can create nsecmdpart afresh
   * run this
   * run FileBasedKafkaPartProducer
   * Track nsecmdpart through kafka-console-consumer
   */
  val spark = SparkSession
    .builder
    .appName("StructuredKafkaWordCount")
    .config("spark.sql.shuffle.partitions", "3")
    .master("local[*]")
    .getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")

  import spark.implicits._
  addStreamingQueryListeners(spark, true)
  val endingOffsetsToBeginWith = """{"nsecmdpart":{"2":0,"1":0,"0":485}}"""
  val stockQuotes =
    spark.readStream.format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "nsecmdpart")
      .option("failOnDataLoss", "false")
      //      .option("startingOffsets", "latest")
      // we could explicitly set offsets
      .option("startingOffsets", endingOffsetsToBeginWith)
      //      .option("auto.offset.reset", "latest")
      .load()
      .select("KEY", "VALUE", "TIMESTAMP")

  val stocksDF = stockQuotes.as[(String, String, Timestamp)]
    .map(_._2 split ",")
    .map(x => Stock(x(0), x(2).toDouble, x(8).toInt, x(4).toDouble))
    .toDF

  val stocksAggregated = stockQuotes.as[(String, String, Timestamp)]
    .map(x => (x._2.split(","), x._3))
    .map(x => (x._1(0), x._1(5).toDouble, x._1(8).toLong, x._1(9).toDouble, x._2))
    .toDF("symbol", "close", "qty", "value", "tstamp")
    .withWatermark("tstamp", "30 seconds")
    .groupBy($"symbol")
    .agg(avg($"qty").as("avgqty"), avg($"value").as("avgval"),
      sum($"qty").as("totqty"), sum($"value").as("sumval"),
      min($"close").as("mincls"), max($"close").as("maxcls"))
    .toDF("symbol", "avgqty", "avgval", "totqty", "sumval", "mincls", "maxcls")
    .filter("""symbol = 'ITC' or symbol = 'TCS' or symbol = 'CIPLA' or
      symbol = 'AUROPHARMA' or symbol='BHARTIARTL'""")
    .coalesce(1)

  stocksAggregated.writeStream
    .format("console")
    .option("truncate", false)
    .option("numRows", 1000)
    .outputMode("update")
    .trigger(ProcessingTime("10 seconds"))
    //.option("path", "hdfs://localhost:8020/user/cloudera/stocksaggplain")
    .option("checkpointLocation", "file:///home/cloudera/stocksaggplainckpt")
    .start
    .awaitTermination()

  /*
   * hive table to create to read the parquet directory
      	create external table stocksaggplain(symbol string,
       	avgqty double, avgval double, totqty bigint, sumval double, mincls double,
       	maxcls double) stored as parquet
      	 location 'hdfs://localhost:8020/user/cloudera/stocksaggplain';
     */
}
