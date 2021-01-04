package com.skk.training.sparkcore

import org.apache.spark.sql.SparkSession

object WordCountMapPartitions extends App {
  val spark = SparkSession.builder().master("local").getOrCreate()
  val sc = spark.sparkContext
  sc.setLogLevel("ERROR")

  val textFileLocation = "file:///D:/ufdata/shakespeare.txt"
  val wordsFile = sc.textFile(textFileLocation)
  wordsFile.cache

  // straight reduceByKey and sort
  println("top 20 words after straight reduce by key and sort")
  wordsFile.flatMap(_ split " ").filter(_ != "").map(
    (_, 1)).reduceByKey(_ + _).sortBy(-_._2).take(20).foreach(println)

  // using map partitions
  val wcMapParts = wordsFile.flatMap(_ split " ").filter(_ != "").mapPartitions { case witer =>
    val wcMap = scala.collection.mutable.Map[String, Int]()
    witer.foreach {
      case word => if (wcMap.isDefinedAt(word)) wcMap(word) = wcMap(word) + 1 else wcMap(word) = 1
    }
    wcMap.iterator
  }

  println("printing top 20 from mappartitions collected and grouped")
  wcMapParts.glom.flatMap(x => x)
    .collect.groupBy(_._1)
    .map(x => (x._1, x._2.map(t => t._2).sum))
    .toArray
    .sortBy(x => (-x._2, x._1))
    .take(20)
    .foreach(println)

  println("printing top 20 from mappartitions collected and grouped employing case notation partial functions to make the logic more explicit")
  wcMapParts.glom.flatMap(partArray => partArray).collect.groupBy {
        // flatampping the partitions gives us flattened collection of (word, count) tuples
        // we group them by word to get a collection of word and tuples for the word
    case (word, _) => word
  }.map {
        // we have now word and tuples for the word
    case (word, iterOfTuples) =>
      // from the tuples of word and iterator of (word, count) tuples
      // we take the word from the first part, count from the second part of the
      // tuples in the iterator and sum it
      (word, iterOfTuples.map {
        case (_, inumber) => inumber
      }.sum)
  }.toArray.sortBy {
        // we sort the collected word, count tuples by the count descending
    case (word, number) => (-number, word)
  }.take(20).foreach(println)

  println("Execution finished. Review the web ui, if needed, and stop the application")
  Thread.sleep(10000000)
}
