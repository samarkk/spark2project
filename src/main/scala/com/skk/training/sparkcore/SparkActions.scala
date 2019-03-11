package com.skk.training.sparkcore
import org.apache.spark.sql.SparkSession

object SparkActions extends App {
  val spark = SparkSession.builder()
    .appName("SparkActions")
    .master("local[*]")
    .getOrCreate()

  val sc = spark.sparkContext
  //  println(sc.version + " , " + spark.version)
  sc.setLogLevel("ERROR")

  val ardd = sc.parallelize(1 to 4)
  val pairRDD = ardd.map(x => (x, x))

  println("Reduce: " + ardd.reduce(_ + _))
  println("Collect: " + ardd.collect())
  println("Keys: " + pairRDD.keys.collect.mkString(","))
  println("Values: " + pairRDD.values.collect().mkString(","))
  println("Aggregate: " + ardd.aggregate((0, 0))((acc, value) => (acc._1 + 1, acc._2 + value),
    (a, b) => (a._1 + b._1, a._2 + b._2)))
  println("First: " + pairRDD.first())
  println("Take: " + ardd.take(2).mkString(","))
  println("Foreach: ")
  ardd.foreach(println)
  println("Top: " + ardd.sortBy(x => x, false).top(2).mkString(","))
  println("CollectAsMap " + pairRDD.collectAsMap().mkString(","))
  println("Count: " + ardd.count)
  println("Count by key - flatmap and then: " +
    ardd.flatMap(_ to 5).map(x => (x, x)).countByKey.mkString(","))
  println("Count by key - flatmap and then: " +
    ardd.flatMap(_ to 5).map(x => (x, x)).countByValue.mkString(","))
  println("Take Sample: " + ardd.takeSample(true, 2, 5).mkString(","))

  import org.apache.spark.rdd.RDD
  val arddDbl: RDD[Double] = sc.parallelize(List(1, 2, 3, 4))
  println("Min: " + ardd.min() + ", Max: " + ardd.max() + " Sum: " + ardd.sum())
  println("Mean: " + arddDbl.mean() + ", StDev: " + arddDbl.stdev() + ", Variance: " +
    arddDbl.variance())
  println("All together in stats: " + arddDbl.stats())

  val arand = new scala.util.Random
  val randList = (for (x <- 1 to 100) yield arand.nextInt(100).toDouble).toList
  val randRDD: RDD[Double] = sc.parallelize(randList)
  val histogram = randRDD.histogram(10)
  println("Histogram buckets: " + histogram._1.mkString(","))
  println("Histogram frequency: " + histogram._2.mkString(","))

  val approxRDD = sc.parallelize(1 to 10000000).map {
    x =>
      (if (x % 2 == 0) "even"
      else "odd", x)
  }

  //def countApprox(timeout: Long, confidence: Double = 0.95): PartialResult[BoundedDouble]
  println("countApprox: " + approxRDD.countApprox(100, 0.999).getFinalValue())

  //  val randRDDForApprox = sc.parallelize(for (x <- 1 to 10000000) yield arand.nextInt(1000000))
  //  println("Distinc element count without approx: " + randRDDForApprox.distinct().count())
  //  println("countApproxDistinct: " + randRDDForApprox.countApproxDistinct(0.95))
  import org.apache.hadoop.io._
  import org.apache.hadoop.conf.Configuration
  println("\nSaving pairRDD to text file - directory will be created with one file for each partition")
  pairRDD.saveAsTextFile("file:///D:/ufdata/prdd_text")
  println("Loading from the saved text file")
  sc.textFile("file:///D:/ufdata/prdd_text").collect().foreach(println)

  //  println("Using the hadoop file method - text file is a wrapper for this method")
  //  val hconf = new Configuration(sc.hadoopConfiguration)
  //  sc.newAPIHadoopFile("file:///D:/ufdata/prdd_text",
  //    classOf[org.apache.hadoop.mapreduce.lib.input.TextInputFormat], classOf[LongWritable],
  //    classOf[Text], hconf).collect().foreach(println)

  println("\nSaving pairRDD to object file - directory will be created with one file for each partition")
  pairRDD.saveAsObjectFile("file:///D:/ufdata/prdd_object")
  println("Loading from the saved object file")
  sc.objectFile("file:///D:/ufdata/prdd_object").collect().foreach(println)

  println("\nSaving pairRDD as a sequence file")
  pairRDD.saveAsSequenceFile("file:///D:/ufdata/prdd_sequence")
  println("Loading from the saved sequence file")
  sc.sequenceFile[Int, Int]("file:///D:/ufdata/prdd_sequence").collect.foreach(println)

  import spark.implicits._

  val pairDF = pairRDD.toDF("k", "v")
  pairDF.collect().foreach(println)
  pairDF.write.parquet("file:///D:/ufdata/prdd_parquet")
  spark.read.parquet("file:///D:/ufdata/prdd_parquet").
    collect.
    foreach(println)
}
