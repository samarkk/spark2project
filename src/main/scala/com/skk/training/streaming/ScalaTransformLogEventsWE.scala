package com.skk.training.streaming
import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.SparkConf
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.flume.FlumeUtils
import java.net.InetSocketAddress
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.rdd.RDD

object ScalaTransformLogEventsWE {

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("Spark Flume Log Analyzer").setMaster("local[*]")
    val strc = new StreamingContext(conf, Seconds(5))
    strc.sparkContext.setLogLevel("WARN")

    var addresses = new Array[InetSocketAddress](1)
    addresses(0) = new InetSocketAddress("localhost", 34500)

    val fluStream = FlumeUtils.createPollingStream(strc, addresses,
      StorageLevel.MEMORY_AND_DISK, 1000, 1)
    //    fluStream.print()

    val transformLog = new WEScalaLogAnalyzer
    /*
 		val lomaps = List(Map((2, 4), (3, 9)), Map((2, 8), (3, 27)))
 		val lomapsRDD = sc.parallelize(lomaps)
 		lomapsRDD.flatMap(x => x)
		lomapsRDD.flatMap(x => x).collect
		Array[(Int, Int)] = Array((2,4), (3,9), (2,8), (3,27))
 */
    val newDstream = fluStream.flatMap {
      x =>
        transformLog.transformLogData(
          new String(x.event.getBody.array()))
          .filter(_._1 != "")
    }

    //    newDstream.print()
    println("beginning transformations")
    executeTransformations(newDstream, strc)

    strc.start()
    strc.awaitTermination()
  }
  def executeTransformations(dstream: DStream[(String, String)], strc: StreamingContext) {
    println("Priting all log values")
    printLogValues(dstream, strc)

    // print total number of requests
    dstream.filter(_._1 == "method").count()
      .map("\nCount of total number of requests: " + _).print()

    // print total number of get requests
    dstream.filter(x => x._1 == "method" && x._2 == "GET").count()
      .map("\nCount of total number of get requests: " + _).print()

    // print the count of various requests in a dstream
    dstream.filter(_._1 == "method").map(x => (x._2, 1))
      .reduceByKey(_ + _).map(x => ("Request type: " + x._1, x._2)).print()

    // Transform functions
    println("\nPriting transformations")
    val transformedRDD = dstream.transform(functionCountRequestType)
    strc.checkpoint("checkpointDir")
    transformedRDD.updateStateByKey(functionTotalCount).map(x => ("Total so for from"
      + " update state operation for request type: " + x._1, x._2)).print()

    // Window operations
    executeWindowingOperations(dstream, strc)
  }

  def printLogValues(dstream: DStream[(String, String)], strc: StreamingContext) {
    dstream.foreachRDD(foreachFunc)
    def foreachFunc = (rdd: RDD[(String, String)]) => {
      val array = rdd.collect()
      println("\nnew array going to be printed " + array.size + " length of method pairs "
        + array.filter(_._1 == "method").length + " " + array.mkString(","))
    }
  }

  val functionCountRequestType = (rdd: RDD[(String, String)]) => {
    rdd.filter(_._1 == "method").map(x => (x._2, 1)).reduceByKey(_ + _)
  }

  val functionTotalCount = (values: Seq[Int], state: Option[Int]) =>
    Option(values.sum + state.sum)

  def executeWindowingOperations(dstream: DStream[(String, String)], strc: StreamingContext) {
    val wStream = dstream.window(Seconds(40), Seconds(20))
    wStream.filter(_._1 == "respCode").map(x => (x._2, 1))
      .reduceByKey(_ + _).map(x => ("Window Op Response Code: " + x._1, x._2)).print

    dstream.filter(_._1 == "respCode").map(x => (x._2, 1))
      .reduceByKeyAndWindow((x: Int, y: Int) => x + y, Seconds(40), Seconds(20))
      .map(x =>
        ("Window Op using reduceByKeyAndWindow Response Code: " + x._1, x._2)).print

  }

}
