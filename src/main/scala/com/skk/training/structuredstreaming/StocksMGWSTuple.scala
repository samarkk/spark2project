package com.skk.training.structuredstreaming

import org.apache.spark.sql.SparkSession
import java.sql.Timestamp
import org.apache.spark.sql.streaming.GroupStateTimeout
import org.apache.spark.sql.streaming.GroupState
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.ProcessingTime
import scala.reflect.api.materializeTypeTag

object StocksMGWSTuple extends App {
  val spark = SparkSession
    .builder
    .appName("StocksMapGroupsWithStateMultipleFieldsKeys")
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
    .map(x => StockEventT(
      (x.getAs[String](1), x.getAs[String](2)),
      x.getAs[String](3).toInt,
      x.getAs[String](4).toDouble,
      x.getAs[Timestamp](0)))

  //  stocks.groupBy($"stockId").agg(sum($"chgoi").as("totoi"), sum($"vlkh").as("totval"))
  //    .writeStream.format("console").outputMode("update").start.awaitTermination

  val stockUpdates = stocks.groupByKey(x => x.stockId)
    .mapGroupsWithState[StockState, StockUpdateT](
      GroupStateTimeout.ProcessingTimeTimeout) {
        case (stockId: (String, String), events: Iterator[StockEventT], state: GroupState[StockState]) =>
          if (state.hasTimedOut) {
            val finalUpdate = StockUpdateT(stockId, state.get.duration, state.get.numEvents, state.get.totoi, state.get.totval, expired = true)
            state.remove()
            finalUpdate
          } else {
            val timestamps = events.map(_.ts.getTime).toSeq
            // this is a stream if we call events.length it seems the counter
            // is advanced and we get empty results
            //            println("Number of events " + events.length)
            //            events.take(events.length).foreach(println)
            println(events.map(x => (x.vlkh, x.chgoi)).toSeq)
            // to get multiple values we have to get them together
            // because the moment we call an action the lazy stream is processed
            // and the subsequent value is empty
            val valsToTrack = events.map(x => (x.vlkh, x.chgoi)).toSeq
            //            val totvals = valsToTrack.map(_._1).sum
            //            val oivals = valsToTrack.map(_._2).sum

            //            println(events.map(_.chgoi).toSeq.sum)
            // spent quite some time banging my head around with this
            // if one appended sum to toSeq only the first value got updated
            // the second one remained blank
            // the same lazy stream getting evaluated logic would apply here too
            val totvals = events.map(_.vlkh).toSeq
            val oivals = events.map(_.chgoi).toSeq
            val updatedSession = if (state.exists) {
              val oldSession = state.get
              StockState(
                oldSession.numEvents + timestamps.size, oivals.sum, totvals.sum,
                oldSession.startms,
                math.max(oldSession.endms, timestamps.max))
            } else {
              StockState(timestamps.size, oivals.sum, totvals.sum, timestamps.min, timestamps.max)
            }
            state.update(updatedSession)
            state.setTimeoutDuration("5 seconds")
            StockUpdateT(stockId, state.get.duration, state.get.numEvents, state.get.totoi, state.get.totval, false)
          }
      }

  val query = stockUpdates
    .withColumn(
      "avgoi",
      when($"numEvents" === 0, 0).otherwise($"totoi" / $"numEvents"))
    .withColumn(
      "avgval",
      when($"numEvents" === 0, 0).otherwise($"totval" / $"numEvents")).writeStream
    .outputMode("update")
    .trigger(ProcessingTime("10 seconds"))
    .format("console")
    .start

  query.awaitTermination()

}
