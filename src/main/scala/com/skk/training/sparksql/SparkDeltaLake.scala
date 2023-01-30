package com.skk.training.sparksql

import org.apache.spark.sql.SparkSession
import io.delta.tables._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger


/*
to execut the package with spark-shell
spark-shell --packages io.delta:delta-core_2.13:2.2.0 --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog"
 */
object SparkDeltaLake extends App {
  val spark = SparkSession.builder().appName("SparkDeltaLakeDemo")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog").
    getOrCreate()

  import spark.implicits._

  val sc = spark.sparkContext
  sc.setLogLevel("WARN")
  val tempDataLocation = args(0)

  // To create a Delta table, write a DataFrame out in the delta format.
  // You can use existing Spark SQL code and change the format
  // from parquet, csv, json, and so on, to delta.
  val data = spark.range(0, 5)
  data.write.format("delta").mode("overwrite").save(tempDataLocation)

  val df = spark.read.format("delta").load(tempDataLocation)
  df.show()

  // Conditional update without overwrite
  //Delta Lake provides programmatic APIs to conditional update, delete, and merge (upsert) data into tables
  val deltaTable = DeltaTable.forPath(tempDataLocation)

  // Update every even value by adding 100 to it
  deltaTable.update(
    condition = expr("id % 2 == 0"),
    set = Map("id" -> expr("id + 100")))

  // Delete every even value
  deltaTable.delete(condition = expr("id % 2 == 0"))
  // Upsert (merge) new data
  val newData = spark.range(0, 20).toDF

  deltaTable.as("oldData")
    .merge(
      newData.as("newData"),
      "oldData.id = newData.id")
    .whenMatched
    .update(Map("id" -> col("newData.id")))
    .whenNotMatched
    .insert(Map("id" -> col("newData.id")))
    .execute()

  deltaTable.toDF.show()

  //  Read older versions of data using time travel
  //  You can query previous snapshots of your Delta table by using time travel.
  //  If you want to access the data that you overwrote, you can query a
  //  snapshot of the table before you overwrote the first set of data using the versionAsOf option

  val dfVer0 = spark.read.format("delta").option("versionAsOf", 0).load("/tmp/delta-table")
  dfVer0.show()

  //  Write a stream of data to a table
  //  You can also write to a Delta table using Structured Streaming
  //  By default, streams run in append mode, which adds new records to the table
  //  even when there are other streams or batch queries running concurrently against the table.
  //  The Delta Lake transaction log guarantees exactly-once processing
  val streamCheckPointLocation = args(1)
  val streamTableLocation = args(2)

  val streamingDf = spark.readStream.format("rate").load()
  val stream = streamingDf.select($"value" as "id").writeStream.
    format("delta").
    option("checkpointLocation", streamCheckPointLocation).
    trigger(Trigger.ProcessingTime("5 seconds")).
    start(streamTableLocation)

  spark.read.format("delta").load(streamTableLocation).show(1000, false)
  val stream2 = spark.readStream.format("delta").load(streamTableLocation).writeStream.format("console").start()
}
