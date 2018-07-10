package com.skk.training.kafkastreaming

//import kafka.serializer.StringDecoder
import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{ FileSystem, Path }
import StreamingContext._
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.io.{ FloatWritable, Writable, Text }
import org.apache.hadoop.hbase.client.{ Put }
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.streaming.kafka010.HasOffsetRanges
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies

object RealTimeSalesIngestion {
  def main(args: Array[String]) {

    def updateFunction(values: Seq[Float], runningCount: Option[Float]) = {
      val newCount = values.sum + runningCount.getOrElse(0.0f)
      new Some(newCount)
    }

    val sparkConf = new SparkConf().setAppName("Real Time Sales Ingestion").setMaster("local[2]")
    val ssc = new StreamingContext(sparkConf, Seconds(5))
    ssc.sparkContext.setLogLevel("ERROR")
    val hadoopConf = new org.apache.hadoop.conf.Configuration()
    hadoopConf.set("fs.hdfs.impl", classOf[org.apache.hadoop.hdfs.DistributedFileSystem].getName)
    hadoopConf.set("fs.file.impl", classOf[org.apache.hadoop.fs.LocalFileSystem].getName)
    val hdfs = FileSystem.get(new java.net.URI("hdfs://localhost:8020"), hadoopConf)

    // Create direct kafka stream with brokers and topics
    val brokers = "localhost:9092"
    val topics = "salestopic"
    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, String](
      "metadata.broker.list" -> brokers,
      "bootstrap.servers" -> brokers,
      "group.id" -> "whatever",
      "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer")

    //    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
    //      ssc, kafkaParams, topicsSet)
    val messages = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent, ConsumerStrategies.Subscribe[String, String](topicsSet, kafkaParams))
    //    val lines = messages.map(x => x.value())
    val lines = messages.map(_.value())

    ssc.checkpoint("hdfs://localhost:8020/spark-demo/checkpoints")

    val salesDataRDD = lines.map(row => {

      val columnValues = row.split(",")
      val transactionDate = columnValues(0).substring(0, columnValues(0).indexOf(" "))
      val skuKey = columnValues(2)
      val storeid = columnValues(1)
      val salesAmount = columnValues(3).toFloat
      ((transactionDate, skuKey, storeid), salesAmount)
    })

    val salesCount = salesDataRDD.reduceByKey(_ + _)
    val totalSalesCount = salesCount.updateStateByKey(updateFunction _)

    totalSalesCount.foreachRDD(rdd => {

      val conf = HBaseConfiguration.create()
      conf.set(TableOutputFormat.OUTPUT_TABLE, "salesdata")
      conf.set("hbase.zookeeper.quorum", "localhost:2181")
      conf.set("hbase.rootdir", "/usr/lib/hbase")
      val jobConf = new Configuration(conf)
      jobConf.set("mapreduce.job.output.key.class", classOf[Text].getName)
      jobConf.set("mapreduce.job.output.value.class", classOf[FloatWritable].getName)
      jobConf.set("mapreduce.outputformat.class", classOf[TableOutputFormat[Text]].getName)
      /*
       * This affects both mapred ("mapred.output.dir") and
       * mapreduce ("mapreduce.output.fileoutputformat.outputdir")
       * based OutputFormat's which do not set the properties
       *  referenced and is an incompatibility introduced in spark 2.2
			 *  Workaround is to explicitly set the property to a dummy value
			 *	(which is valid and writable by user - say /tmp).
       */
      jobConf.set("mapreduce.output.fileoutputformat.outputdir", "/tmp")
      rdd.map(convert).saveAsNewAPIHadoopDataset(jobConf)
    })

    totalSalesCount.map(a => a._1._1 + "," + a._1._2 + "," + a._1._3 + "," + a._2).print()
    ssc.start()
    ssc.awaitTermination()
  }

  def convert(t: ((String, String, String), Float)) = {
    val p = new Put(Bytes.toBytes(t._1._1 + "-" + t._1._2 + "-" + t._1._3))
    p.add(Bytes.toBytes("count"), Bytes.toBytes("salescount"), (t._2).toString().getBytes())
    (t._1, p)
  }
}
