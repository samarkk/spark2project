package com.skk.training.sparkcore
import org.apache.spark.sql.SparkSession

object SparkS3Access extends App {
  val spark = SparkSession.builder()
    .appName("SparkActions")
    .master("local[*]")
    .getOrCreate()

  val sc = spark.sparkContext
  //  println(sc.version + " , " + spark.version)
  sc.setLogLevel("ERROR")
  sc.hadoopConfiguration.set("fs.s3.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
  sc.hadoopConfiguration.set("fs.s3.awsAccessKeyId", "AKIAJOKZIKXNIEHRB56Q")
  sc.hadoopConfiguration.set(
    "fs.s3.awsSecretAccessKey",
    "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")

  val shakRDD = sc.textFile("s3://fbucketn/shakespeare.txt")
  //  println(shakRDD.count)
  shakRDD
    .flatMap(_ split " ")
    .filter(_ != "")
    .map(_.toLowerCase)
    .map((_, 1))
    .reduceByKey(_ + _)
    .sortBy(-_._2)
    .take(20).foreach(println)
}
