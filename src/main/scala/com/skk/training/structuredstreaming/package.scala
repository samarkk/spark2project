package com.skk.training
import org.apache.spark.sql.streaming.StreamingQueryListener
import org.apache.spark.sql.streaming.StreamingQueryListener.{ QueryProgressEvent, QueryStartedEvent, QueryTerminatedEvent }
import org.apache.spark.sql.SparkSession
import java.sql.Timestamp

package object structuredstreaming {
  case class Event(sessionId: String, timestamp: Timestamp)
  case class SessionInfo(
    numEvents: Int,
    startTimestampMs: Long,
    endTimestampMs: Long) {
    def durationMs: Long = endTimestampMs - startTimestampMs
  }
  case class SessionUpdate(
    id: String,
    durationMs: Long,
    numEvents: Int,
    expired: Boolean)

  case class Stock(symbol: String, close: Double, qty: Int, value: Double)

  case class StockEvent(stockId: String, instr: String, chgoi: Int, vlkh: Double, ts: Timestamp)
  case class StockEventT(stockId: (String, String), chgoi: Int, vlkh: Double, ts: Timestamp)

  case class StockState(numEvents: Int, totoi: Int, totval: Double, startms: Long, endms: Long) { def duration: Long = endms - startms }
  //  case class StockStateE(numEvents: Int, totoi: Int, totval: Double, startms: Timestamp, endms: Timestamp) { def duration: Long = endms.getTime - startms.getTime }
  //  case class StockStateFE(var id: (String, String), numEvents: Int, totoi: Int, totval: Double, var startms: java.sql.Timestamp,
  //    var endms: java.sql.Timestamp)

  case class StockUpdate(id: String, duration: Long, numEvents: Int, totoi: Int, totval: Double, expired: Boolean)
  case class StockUpdateT(id: (String, String), duration: Long, numEvents: Int, totoi: Int, totval: Double, expired: Boolean)
  //  case class StockUpdateTE(id: (String, String), duration: Long, numEvents: Int, totoi: Int, totval: Double, startms: Timestamp, expired: Boolean)

  def addStreamingQueryListeners(spark: SparkSession, progress: Boolean) {
    spark.streams.addListener(new StreamingQueryListener() {
      override def onQueryStarted(queryStarted: QueryStartedEvent): Unit = {
        println("Query started: " + queryStarted.id)
      }
      override def onQueryTerminated(queryTerminated: QueryTerminatedEvent): Unit = {
        println("Query terminated: " + queryTerminated.id)
      }
      override def onQueryProgress(queryProgress: QueryProgressEvent): Unit = {
        if (progress) {
          //          println("Query made progress: " + queryProgress.progress)
          val soffsets = queryProgress.progress.sources.array(0).startOffset
          val eoffsets = queryProgress.progress.sources.array(0).endOffset
          // we could save endOffset to a db and then pick it up
          // when we start the streaming to start from an explicit location
          println(
            "starting offsets: " + soffsets + " , ending offsets:\n" + eoffsets
              + eoffsets.substring(
                eoffsets.indexOf(":") + 2,
                eoffsets.lastIndexOf("}") - 1).split(",").map(_ split ":").map(
                  x => (x(0), x(1))))
        }
      }
    })
  }
}
