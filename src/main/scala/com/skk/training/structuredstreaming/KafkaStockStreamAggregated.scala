package com.skk.training.structuredstreaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming._
import org.apache.spark.sql.functions._
import scala.reflect.api.materializeTypeTag

object KafkaStockStreamAggregated extends App {
  /*
   * run this app
   * and run testkfp.sh - refer to that program in /home/cloudera for
   * what it does and how to run it
   * the difference between this and KafkaStockStreamKafka is
   * the streaming aggregations are written to a kafka topic there
   *
   */
  val spark = SparkSession
    .builder
    .appName("KafkaStockAggregation")
    .master("local[*]")
    .getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")

  import spark.implicits._

  val stockQuotes =
    spark.readStream.format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "nsecmd")
      .option("startingffsets", "earliest")
      .load()
      .select("KEY", "VALUE")

  val stocksDF = stockQuotes.as[(String, String)]
    .map(_._2 split ",")
    .map(x => Stock(x(0), x(2).toDouble, x(8).toInt, x(4).toDouble))
    .toDF

  val stocksAggregated = stocksDF.groupBy($"symbol")
    .agg(avg($"qty").as("avgqty"), avg($"value").as("avgval"),
      sum($"qty").as("totqty"), sum($"value").as("sumval"),
      min($"close").as("mincls"), max($"close").as("maxcls"))

  stocksAggregated.writeStream
    .format("console")
    .outputMode("complete")
    .option("truncate", false)
    .start
    .awaitTermination()
}
