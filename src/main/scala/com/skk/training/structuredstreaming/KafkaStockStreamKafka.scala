package com.skk.training.structuredstreaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming._
import org.apache.spark.sql.functions._
import scala.reflect.api.materializeTypeTag

object KafkaStockStreamKafka extends App {
  /*
   * this guy  reads from a kafka topic
   * and writes to a kafka topic
   * start this here
   * start testkfp program in one window
   * and for fun start kafka console consumer for nsecmdaggr in another
   * run testkfp.sh from /home/cloudera
   * ./testkfp.sh /home/cloudera/findata/cm 2016 JAN 20 2
   * see it aggregating stuff to nsecmd topic
   * kafka-console-consumer --bootstrap-server localhost:9092 --topic nsecmd --from-beginning
   * run the program and see aggregations coming together in nsecmdaggr
   * kafka-console-consumer --bootstrap-server localhost:9092 --topic nsecmdaggr --from-beginning
   */
  val spark = SparkSession
    .builder
    .appName("StructuredKafkaWordCount")
    .master("local[*]")
    .getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")

  import spark.implicits._

  val stockQuotes =
    spark.readStream.format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "nsecmd")
      .option("startingffsets", "earliest")
      .option("failOnDataLoss", "false")
      .load()
      .select("KEY", "VALUE")

  val stocksDF = stockQuotes.as[(String, String)]
    .map(_._2 split ",")
    .map(x => Stock(x(0), x(2).toDouble, x(8).toInt, x(4).toDouble))
    .toDF

  val stocksAggregated = stocksDF.groupBy($"symbol")
    .agg(avg($"qty").as("avgqty"), avg($"value").as("avgval"),
      sum($"qty").as("totqty"), sum($"value").as("totval"),
      min($"close").as("mincls"), max($"close").as("maxcls"))
  val stocksAggregatedForKafka = stocksAggregated.select(
    $"symbol".cast("string").as("key"),
    concat($"symbol", lit(","), $"avgqty", lit(","), $"avgval",
      lit(","), $"totqty", lit(","), $"totval", lit(","),
      $"mincls", lit(","), $"maxcls").as("value"))

  stocksAggregatedForKafka.writeStream
    .format("kafka")
    .outputMode("update")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("topic", "nsecmdaggr")
    .option("checkpointLocation", "hdfs://localhost:8020/user/cloudera/stcokskafkackpt")
    .start
    .awaitTermination()
}
