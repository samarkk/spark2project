package com.skk.training.structuredstreaming

import org.apache.spark.sql.SparkSession
import java.sql.Timestamp
import org.apache.spark.sql.streaming.GroupStateTimeout
import org.apache.spark.sql.streaming.GroupState
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.ProcessingTime
import scala.reflect.api.materializeTypeTag

object StocksMGWS extends App {
  val spark = SparkSession
    .builder
    .appName("StocksMapGroupsWithState")
    .master("local[*]")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")
  import spark.implicits._

  val stocks = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "localhost:9092").option("subscribe", "nsefod").option("spark.sql.shuffle.partitions", "2").load().withColumn("ts", current_timestamp()).select("ts", "value").as[(Timestamp, String)]
    .withColumn("stockId", split(col("value"), ",").getItem(1))
    .withColumn("instr", split(col("value"), ",").getItem(0))
    .withColumn("chgoi", split(col("value"), ",").getItem(12))
    .withColumn("vlkh", split(col("value"), ",").getItem(11))
    .drop("value")
    .map(x => StockEvent(
      x.getAs[String](1),
      x.getAs[String](2),
      x.getAs[String](3).toInt,
      x.getAs[String](4).toDouble,
      x.getAs[Timestamp](0)))

  //  stocks.groupBy($"stockId").agg(sum($"chgoi").as("totoi"), sum($"vlkh").as("totval"))
  //    .writeStream.format("console").outputMode("update").start.awaitTermination

  val stockUpdates = stocks.groupByKey(x => x.stockId)
    .mapGroupsWithState[StockState, StockUpdate](
      GroupStateTimeout.ProcessingTimeTimeout) {
        case (stockId: String, events: Iterator[StockEvent], state: GroupState[StockState]) =>
          if (state.hasTimedOut) {
            val finalUpdate = StockUpdate(stockId, state.get.duration, state.get.numEvents, state.get.totoi, state.get.totval, expired = true)
            state.remove()
            finalUpdate
          } else {
            val timestamps = events.map(_.ts.getTime).toSeq
            val oivals = events.map(_.chgoi).toSeq.sum
            val totvals = events.map(_.chgoi).toSeq.sum
            val updatedSession = if (state.exists) {
              val oldSession = state.get
              StockState(
                oldSession.numEvents + timestamps.size, oivals, totvals,
                oldSession.startms,
                math.max(oldSession.endms, timestamps.max))
            } else {
              StockState(timestamps.size, oivals, totvals, timestamps.min, timestamps.max)
            }
            state.update(updatedSession)
            state.setTimeoutDuration("10 seconds")
            StockUpdate(stockId, state.get.duration, state.get.numEvents, state.get.totoi, state.get.totval, false)
          }
      }

  val query = stockUpdates
    .withColumn(
      "avgoi",
      when($"numEvents" === 0, 0).otherwise($"totoi" / $"numEvents")).writeStream
    .outputMode("update")
    .trigger(ProcessingTime("20 seconds"))
    .format("console")
    .start

  query.awaitTermination()

}
