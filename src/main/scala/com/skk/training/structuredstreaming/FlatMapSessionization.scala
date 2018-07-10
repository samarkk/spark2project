package com.skk.training.structuredstreaming
import org.apache.spark.sql.SparkSession
import java.sql.Timestamp
import org.apache.spark.sql.streaming.{ GroupStateTimeout, OutputMode }
import org.apache.spark.sql.streaming.GroupState
import scala.Iterator
import scala.reflect.api.materializeTypeTag

object FlatMapSessionization extends App {
  val spark = SparkSession
    .builder
    .appName("StructuredSessionization")
    .master("local[*]")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")
  import spark.implicits._

  // Create DataFrame representing the stream of input lines from connection to host:port
  val lines = spark.readStream
    .format("socket")
    .option("host", "localhost")
    .option("port", 9999)
    .option("includeTimestamp", true)
    .load()

  val events = lines.as[(String, Timestamp)]
    .flatMap(x => x._1.split(" ")
      .map(word => Event(word, x._2)))

  val sessionUpdates = events
    .groupByKey(event => event.sessionId)
    .flatMapGroupsWithState[SessionInfo, SessionUpdate](
      outputMode = OutputMode.Append,
      timeoutConf = GroupStateTimeout.ProcessingTimeTimeout()) {
        case (sessionId: String, events: Iterator[Event], state: GroupState[sessionInfo]) =>
          if (state.hasTimedOut) {
            val finalUpdate = SessionUpdate(sessionId, state.get.durationMs,
              state.get.numEvents, expired = true)
            state.remove()
            Iterator(finalUpdate)
          } else {
            val timestamps = events.map(_.timestamp.getTime).toSeq
            val updatedSession = if (state.exists) {
              val oldSession = state.get
              SessionInfo(
                oldSession.numEvents + timestamps.size,
                oldSession.startTimestampMs,
                math.max(oldSession.endTimestampMs, timestamps.max))
            } else {
              SessionInfo(timestamps.size, timestamps.min, timestamps.max)
            }
            state.update(updatedSession)
            state.setTimeoutDuration("5 seconds")
            Iterator(
              SessionUpdate(sessionId, state.get.durationMs,
                state.get.numEvents, false))
          }
      }

  val query = sessionUpdates.writeStream
    .outputMode("append")
    .format("console")
    .start

  query.awaitTermination()
}
