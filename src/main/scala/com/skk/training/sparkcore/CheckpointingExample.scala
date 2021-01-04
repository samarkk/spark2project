package com.skk.training.sparkcore

import org.apache.spark.sql.SparkSession


object CheckpointingExample extends App {

  /*
  Checkpointing vs caching
  No memory only disk
  more space, slower than caching
  dependency graph is erased
  disk location is usually a cluster available file system such as HDFS
  node failure with caching - partition is lost and needs to be recomputed
  node failure with checkpoinging - partition is reloaded on another executor
   */
  val spark = SparkSession.builder()
    .appName("SparkCoreTransformations")
    .master("local")
    .getOrCreate()

  val sc = spark.sparkContext
  val chkptDir = "hdfs://localhost:8020/user/samar/ckptdir"
  sc.setCheckpointDir(chkptDir)

  val logsFileLocation = "file:///D:/ufdata/apachelogs"

  def parseApacheLogLine(logLine: String): LogRecord = {
    val AALP = """^(\S+) (\S+) (\S+) \[([\w:/]+\s[+\-]\d{4})\] "(\S+) (\S+)\s*(\S*)\s*" (\d{3}) (\S+)""".r
    val res = AALP.findFirstMatchIn(logLine)

    try {
      val m = res.get
      LogRecord(m.group(1), m.group(2), m.group(3), m.group(4), m.group(5), m.group(6),
        m.group(7), m.group(8), m.group(9) match { case "-" => 0; case x => x.toLong })
    } catch {
      case ex: Exception => null
    }
  }

  val logsRDD = sc.textFile(logsFileLocation).map(parseApacheLogLine)
  logsRDD.checkpoint()
  println(logsRDD.count)

  import spark.implicits._

  val logsDF = spark.read.text(logsFileLocation).map { x =>
    x.getAs[String](0)
  }
  logsDF.show()

  val logsCKDF = logsDF.checkpoint()
  logsCKDF.show()

  println("Thread about to go to sleep")
  Thread.sleep(10000000)
  /*
  use checkpointing if recomputation is expensive, a job is failing
   */
}
