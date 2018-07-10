package com.skk.training.structuredstreaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming._
import org.apache.spark.sql.functions._
import java.sql.Timestamp
import scala.reflect.api.materializeTypeTag

object KafkaStockStreamAggParquet extends App {
  /*
   * empty the hive table
   * empty the checkpoint directory
   * if need be can create nsecmd afresh
   * run this
   * run testkfp
   * run hive - initially you may have parquet file is too small messages
   * stuff will accumulate and results will be visible with select
   */
  val spark = SparkSession
    .builder
    .appName("KafkaStreamAggregatedToParquet")
    .config("spark.sql.shuffle.partitions", "3")
    .master("local[*]")
    .getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")

  import spark.implicits._
  addStreamingQueryListeners(spark, true)

  val stockQuotes =
    spark.readStream.format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "nsecmd")
      .option("startingffsets", "latest")
      //      .option("auto.offset.reset", "latest")
      .load()
      .select("KEY", "VALUE", "TIMESTAMP")

  val stocksDF = stockQuotes.as[(String, String, Timestamp)]
    .map(_._2 split ",")
    .map(x => Stock(x(0), x(2).toDouble, x(8).toInt, x(4).toDouble))
    .toDF

  val stocksAggregatedWithWindow = stockQuotes.as[(String, String, Timestamp)]
    .map(x => (x._2.split(","), x._3))
    .map(x => (x._1(0), x._1(5).toDouble, x._1(8).toLong, x._1(9).toDouble, x._2))
    .toDF("symbol", "close", "qty", "value", "tstamp")
    .withWatermark("tstamp", "30 seconds")
    .groupBy($"symbol", window($"tstamp", "10 seconds", "5 seconds"))
    .agg(avg($"qty").as("avgqty"), avg($"value").as("avgval"),
      sum($"qty").as("totqty"), sum($"value").as("sumval"),
      min($"close").as("mincls"), max($"close").as("maxcls"))
    .toDF("symbol", "wdw", "avgqty", "avgval", "totqty", "sumval", "mincls", "maxcls")
    .coalesce(1)

  stocksAggregatedWithWindow.writeStream
    .format("parquet")
    .option("path", "hdfs://localhost:8020/user/cloudera/stocksagg")
    .option("checkpointLocation", "hdfs://localhost:8020/user/cloudera/stocksaggckpt")
    .start
    .awaitTermination()

  /*
   * hive table to create to read the parquet directory
      	create external table stocksagg(symbol string,
       	wdw struct<start:timestamp, `end`:timestamp>, avgqty double,
       	avgval double, totqty bigint, sumval double, mincls double,
       	maxcls double) stored as parquet
      	 location 'hdfs://localhost:8020/user/cloudera/stocksagg';
     */
}
