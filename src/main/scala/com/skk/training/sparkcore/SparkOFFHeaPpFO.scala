package com.skk.training.sparkcore

import org.apache.spark.sql.SparkSession
import scala.util.Random

object SparkOFFHeapTFO extends App {
  val spark = SparkSession.builder()
    .appName("SparkCoreTransformations")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  spark.conf.set("spark.local.dir", "D:\temp")
  spark.conf.set("spark.memory.offHeap.enabled", "true")
  spark.conf.set("spark.memory.offHeap.size", "2G")

  println("Storage memory granted: " +
    spark.sparkContext.getExecutorStorageStatus(0).maxMemory)

  val pw2df = spark.sparkContext.parallelize(1 to 1e7.toInt).map(
    x => (x, x * x)).toDF("nmbr", "sq")
  val pw3df = spark.sparkContext.parallelize(1 to 1e7.toInt).map(
    x => (x, x * x * x)).toDF("nmbr", "cb")
  println(pw2df.join(pw3df, "nmbr").count)
  Thread sleep 100000
}
