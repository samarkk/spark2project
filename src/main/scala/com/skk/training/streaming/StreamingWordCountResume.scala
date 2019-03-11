package com.skk.training.streaming
import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.rdd.RDD
import org.apache.log4j.{ Logger, Level }

object StreamingWordCountResume extends App {
  def funcToCreateContext(): StreamingContext = {
    val conf = new SparkConf().setAppName("StreamingWordCount").setMaster("local[*]")
    val ssc = new StreamingContext(conf, Seconds(10))
    val wordStream = ssc.socketTextStream("localhost", 9999)
    val chkPointDirLocation = "hdfs://localhost:8020/user/cloudera/wcchkpoint"
    ssc.checkpoint(chkPointDirLocation)
    val wordCounts = wordStream.flatMap(_ split " ").map((_, 1)).reduceByKey(_ + _)
    wordCounts.print()
    val stateWindowCounts = wordCounts.updateStateByKey((values: Seq[Int], state: Option[Int]) => Option(values.sum + state.sum))
    val stateFP = stateWindowCounts.map(x => "stateWordCounts " + x._1 + "  , " + x._2)
    stateFP.print(1000)
    ssc
  }
  // in case get the error
  // class "javax.servlet.ServletRegistration"'s signer information does not match signer information of other classes in the same package
  // in the build path order and export tab shift javax.servlet to thte top
  //val conf = new SparkConf().setAppName("StreamingWordCount").setMaster("local[*]")
  val chkPointDirLocation = "hdfs://localhost:8020/user/cloudera/wcchkpoint"

  val ssc = StreamingContext.getOrCreate(chkPointDirLocation, funcToCreateContext)

  ssc.sparkContext.setLogLevel("ERROR")

  ssc.start()
  ssc.awaitTermination()

}
