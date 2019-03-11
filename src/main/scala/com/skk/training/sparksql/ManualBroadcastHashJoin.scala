package com.skk.training.sparksql

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Dataset
import org.apache.spark.rdd.RDD
import scala.reflect.ClassTag

object ManualBroadcastHashJoin extends App {
  val spark = SparkSession.builder()
    .appName("MBHK")
    .master("local[*]")
    .getOrCreate()

  val sc = spark.sparkContext
  //  println(sc.version + " , " + spark.version)
  sc.setLogLevel("ERROR")
  import spark.implicits._

  def manualBroadcastHashJoin[K: Ordering: ClassTag, V1: ClassTag, V2: ClassTag](
    bigRDD: RDD[(K, V1)], smallRDD: RDD[(K, V2)]): RDD[(K, (V1, V2))] = {
    val smallRDDLocal = smallRDD.collectAsMap()
    bigRDD.sparkContext.broadcast(smallRDDLocal)
    bigRDD.mapPartitions(
      iter =>
        iter.flatMap {
          case (k, v1) =>
            smallRDDLocal.get(k) match {
              case None     => Seq.empty[(K, (V1, V2))]
              case Some(v2) => Some((k, (v1, v2)))
            }
        })
  }
  val bigRDD = sc.parallelize((1 to 100000).flatMap(x => (1 to 3).map(y => (x, math.pow(x, y)))))
  val tprimes = (2 to 100000).filter(x => (2 to math.sqrt(x).toInt).forall(y => x % y != 0))
  val smallRDD = sc.parallelize(tprimes.map((_, 1)))
  manualBroadcastHashJoin(bigRDD, smallRDD).take(20).foreach(println)
}
