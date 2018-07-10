package com.skk.training.structuredstreaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming._
import org.apache.spark.sql.functions._
import java.sql.Timestamp
import scala.reflect.api.materializeTypeTag

object StructuredKafkaWordCount extends App {
  /* to run this
   * just start it here and start the kafka console producer in the terminal
   * and write to topic wci and we should be good
   * kafka-console-producer --broker-list localhost:9092 --topic wci
   */
  /*
   * to run it from spark-submit need to add --conf spark.driver.extraClassPath etc
   * also we could add the required kafka clients jars to the
   * -- command it to run from the base directory of the project
   * spark-submit --conf
    "spark.driver.extraClassPath=/home/cloudera/.ivy2/cache/org.apache.kafka/kafka-clients/jars/kafka-clients-1.1.0.jar" --conf "spark.executor.extraClassPath=/home/cloudera/.ivy2/cache/org.apache.kafka/kafka-ients/jars/kafka-clients-1.1.0.jar" \
     --class com.skk.training.structuredstreaming.StructuredKafkaWordCount \
      target/scala-2.11/Spark2Project-assembly-0.1-SNAPSHOT.jar
      // command to run it with configs set in spark-defaults.conf
      spark-submit \
      --class com.skk.training.structuredstreaming.StructuredKafkaWordCount \
       target/scala-2.11/Spark2Project-assembly-0.1-SNAPSHOT.jar
   */
  val spark = SparkSession
    .builder
    .appName("StructuredKafkaWordCount")
    .master("local[*]")
    .getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")

  val lines = spark
    .readStream
    //    .format("org.apache.spark.sql.kafka010.KafkaSourceProvider")
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "wci")
    // if we were to change latest to earliest we will see all the outputs
    // and it will be also able to tell us by window as it we are using the kafka timestamp
    .option("startingOffsets", "earliest")
    // .option("startingOffsets", """{"wci":{"0":5}}""")
    //    .option("includeTimestamp", true)
    .load().selectExpr("cast(value as string) word", "timestamp")

  import spark.implicits._
  lines.printSchema()

  lines.as[(String, Timestamp)].flatMap(
    x => (x._1 split " ").map(word => (word, x._2))).
    toDF("word", "timestamp").groupBy(
      window($"timestamp", "4 seconds", "2 seconds"),
      $"word").count()
    .writeStream
    .format("console")
    .outputMode("update")
    .option("truncate", false)
    .start()
    .awaitTermination()
}
