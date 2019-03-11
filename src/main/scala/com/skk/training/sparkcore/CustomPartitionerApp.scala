package com.skk.training.sparkcore
import org.apache.spark.Partitioner
import org.apache.spark.sql.SparkSession
object CustomPartitionerApp extends App {

  val spark = SparkSession.builder().master("local[*]").getOrCreate()
  val sc = spark.sparkContext
  sc.setLogLevel("ERROR")

  // create the custom partitioner extending the partitioner class
  class CustomPartitioner(override val numPartitions: Int) extends Partitioner {
    // code the getPartition method assign basis first letter
    override def getPartition(key: Any): Int = {
      // firstLetterOfKey
      val fletOfKey = key.asInstanceOf[String].toLowerCase.substring(0, 1)
      fletOfKey match {
        case it if "abcdef" contains it   => 0
        case it if "ghijkl" contains it   => 1
        case it if "mnopqr" contains it   => 2
        case it if "stuvwxyz" contains it => 3
        case _                            => 4
      }
    }
    // the equals method
    override def equals(other: scala.Any): Boolean = {
      other match {
        case obj: CustomPartitioner => obj.numPartitions == numPartitions
        case _                      => false
      }
    }
  }
  // create testRDD and verify the
  val testRDD = sc.parallelize(List("raman", "aman", "shaman",
    "waman", "taman", "daman", "$$$", "340"))
  // create a pair rdd to apply the partitioner to
  val trdd = testRDD.map(x => (x, x.length))
  println("No of partitions of testRDD: " + testRDD.getNumPartitions)
  val customPartitionedRDD = trdd.partitionBy(new CustomPartitioner(5))
  println("No of partitions for custom partitioned rdd: "
    + customPartitionedRDD.getNumPartitions)
  customPartitionedRDD.mapPartitionsWithIndex(
    (idx, iter) =>
      iter.toList.map((_, idx)).iterator).collect.foreach(println)
}
