package com.skk.training.streaming
import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.rdd.RDD
import org.apache.log4j.{ Logger, Level }

object StreamingWordCount extends App {
  // in case get the error
  // class "javax.servlet.ServletRegistration"'s signer information does not match signer information of other classes in the same package
  // in the build path order and export tab shift javax.servlet to thte top
  val conf = new SparkConf().setAppName("StreamingWordCount").setMaster("local[*]")
  val ssc = new StreamingContext(conf, Seconds(10))
  ssc.sparkContext.setLogLevel("ERROR")
  val wordStream = ssc.socketTextStream("localhost", 9999)
  val wordCounts = wordStream.flatMap(_ split " ").map((_, 1)).reduceByKey(_ + _)
  wordCounts.print()
  val chkPointDirLocation = "hdfs://localhost:8020/user/cloudera/wcchkpoint"
  ssc.checkpoint(chkPointDirLocation)
  //  val windowWordCounts = wordCounts.window(Seconds(20), Seconds(10))
  //  val windowWordCountsOutput = windowWordCounts.reduceByKeyAndWindow((x: Int, y: Int) => x + y, Seconds(20), Seconds(10))
  //  val wwcoFP = windowWordCountsOutput.map(x => "windowWordCounts " + x._1 + "  , " + x._2)
  //  wwcoFP.print(100)
  val stateWindowCounts = wordCounts.updateStateByKey((values: Seq[Int], state: Option[Int]) => Option(values.sum + state.sum))
  val stateFP = stateWindowCounts.map(x => "stateWordCounts " + x._1 + "  , " + x._2)
  stateFP.print(1000)
  ssc.start()
  ssc.awaitTermination()

}
