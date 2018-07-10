package com.skk.training.structuredstreaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.sql.streaming._
import org.apache.spark.sql.functions._
import scala.reflect.api.materializeTypeTag
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.streaming.kafka010.OffsetRange
import kafka.utils.ZkUtils
import org.apache.kafka.common.TopicPartition

object KafkaOffsetsManage extends App {
  /*
 Save offsets for each batch into HBase
*/
  def saveOffsets(TOPIC_NAME: String, GROUP_ID: String, offsetRanges: Array[OffsetRange],
    hbaseTableName: String, batchTime: org.apache.spark.streaming.Time) = {
    val hbaseConf = HBaseConfiguration.create()
    hbaseConf.addResource("src/main/resources/hbase-site.xml")
    val conn = ConnectionFactory.createConnection(hbaseConf)
    val table = conn.getTable(TableName.valueOf(hbaseTableName))
    val rowKey = TOPIC_NAME + ":" + GROUP_ID + ":" + String.valueOf(batchTime.milliseconds)
    val put = new Put(rowKey.getBytes)
    for (offset <- offsetRanges) {
      put.addColumn(Bytes.toBytes("offsets"), Bytes.toBytes(offset.partition.toString),
        Bytes.toBytes(offset.untilOffset.toString))
    }
    table.put(put)
    conn.close()
  }
  /* Returns last committed offsets for all the partitions of a given topic from HBase in
following  cases.
*/

  def getLastCommittedOffsets(TOPIC_NAME: String, GROUP_ID: String, hbaseTableName: String,
    zkQuorum: String, zkRootDir: String, sessionTimeout: Int, connectionTimeOut: Int): Map[TopicPartition, Long] = {

    val hbaseConf = HBaseConfiguration.create()
    val zkUrl = zkQuorum + "/" + zkRootDir
    val zkClientAndConnection = ZkUtils.createZkClientAndConnection(
      zkUrl,
      sessionTimeout, connectionTimeOut)
    val zkUtils = new ZkUtils(zkClientAndConnection._1, zkClientAndConnection._2, false)
    val zKNumberOfPartitionsForTopic = zkUtils.getPartitionsForTopics(Seq(TOPIC_NAME)).get(TOPIC_NAME).toList.head.size
    zkClientAndConnection._1.close()
    zkClientAndConnection._2.close()

    //Connect to HBase to retrieve last committed offsets
    val conn = ConnectionFactory.createConnection(hbaseConf)
    val table = conn.getTable(TableName.valueOf(hbaseTableName))
    val startRow = TOPIC_NAME + ":" + GROUP_ID + ":" +
      String.valueOf(System.currentTimeMillis())
    val stopRow = TOPIC_NAME + ":" + GROUP_ID + ":" + 0
    val scan = new Scan()
    val scanner = table.getScanner(scan.setStartRow(startRow.getBytes).setStopRow(
      stopRow.getBytes).setReversed(true))
    val result = scanner.next()
    var hbaseNumberOfPartitionsForTopic = 0 //Set the number of partitions discovered for a topic in HBase to 0
    if (result != null) {
      //If the result from hbase scanner is not null, set number of partitions from hbase
      //to the number of cells
      hbaseNumberOfPartitionsForTopic = result.listCells().size()
    }

    val sparkConf = new SparkConf()
      .setIfMissing("master", "local")

    val ssc = new StreamingContext(sparkConf, Seconds(10))
    val fromOffsets = collection.mutable.Map[TopicPartition, Long]()

    if (hbaseNumberOfPartitionsForTopic == 0) {
      // initialize fromOffsets to beginning
      for (partition <- 0 to zKNumberOfPartitionsForTopic - 1) {
        fromOffsets += (new TopicPartition(TOPIC_NAME, partition) -> 0)
      }
    } else if (zKNumberOfPartitionsForTopic > hbaseNumberOfPartitionsForTopic) {
      // handle scenario where new partitions have been added to existing kafka topic
      for (partition <- 0 to hbaseNumberOfPartitionsForTopic - 1) {
        val fromOffset = Bytes.toString(result.getValue(
          Bytes.toBytes("offsets"),
          Bytes.toBytes(partition.toString)))
        fromOffsets += (new TopicPartition(TOPIC_NAME, partition) -> fromOffset.toLong)
      }
      for (partition <- hbaseNumberOfPartitionsForTopic to zKNumberOfPartitionsForTopic - 1) {
        fromOffsets += (new TopicPartition(TOPIC_NAME, partition) -> 0)
      }
    } else {
      //initialize fromOffsets from last run
      for (partition <- 0 to hbaseNumberOfPartitionsForTopic - 1) {
        val fromOffset = Bytes.toString(result.getValue(
          Bytes.toBytes("offsets"),
          Bytes.toBytes(partition.toString)))
        fromOffsets += (new TopicPartition(TOPIC_NAME, partition) -> fromOffset.toLong)
      }
    }
    scanner.close()
    conn.close()
    fromOffsets.toMap
  }

  val spark = SparkSession
    .builder
    .appName("KafkaStockAggregation")
    .master("local[*]")
    .getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")

  import spark.implicits._

  val stockQuotes =
    spark.readStream.format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "nsecmdpart")
      .option("startingffsets", "earliest")
      .load()
      .select("KEY", "VALUE")

  val stocksDF = stockQuotes.as[(String, String)]
    .map(_._2 split ",")
    .map(x => Stock(x(0), x(2).toDouble, x(8).toInt, x(4).toDouble))
    .toDF

  val stocksAggregated = stocksDF.groupBy($"symbol")
    .agg(avg($"qty").as("avgqty"), avg($"value").as("avgval"),
      sum($"qty").as("totqty"), sum($"value").as("sumval"),
      min($"close").as("mincls"), max($"close").as("maxcls"))

  stocksAggregated.writeStream
    .format("console")
    .outputMode("complete")
    .option("truncate", false)
    .start
    .awaitTermination()
}
