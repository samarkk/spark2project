package com.skk.training.sparkcore

import org.apache.spark.sql.SparkSession

object SparkMostCommonProductCombinationsCSVF extends App {
  val spark = SparkSession.builder()
    .appName("SparkMostCommonProductCombinationsCSVF")
    .master(args(0))
    .getOrCreate()

  //spark.conf.set("spark.local.dir", "D:\temp")
  val sc = spark.sparkContext
  //  println(sc.version + " , " + spark.version)

  sc.setLogLevel("ERROR")

  val products = sc.textFile("products.csv").map(_ split ",").map(x => (x(0).toInt, x(2)))
  val order_items = sc.textFile("order_items.csv").map(_.split(",")).map(x => (x(2).toInt,
    (x(1).toInt, x(3).toInt)))
  val orders_products = order_items.join(products)
  val orders = orders_products.map {
    case (pid, ((orderid, orderquantity), productname)) => (orderid, (orderquantity, productname))
  }.groupByKey

  val cooccurrences = orders.map(order =>
    (
      order._1,
      order._2.toList.combinations(2).map(order_pair =>
        (
          if (order_pair(0)._2 < order_pair(1)._2)
            (order_pair(0)._2, order_pair(1)._2)
          else
            (order_pair(1)._2, order_pair(0)._2),
          order_pair(0)._1 * order_pair(1)._1))))

  val combos = cooccurrences.flatMap(x => x._2).reduceByKey((a, b) => a + b)
  val mostCommon = combos.map(x => (x._2, x._1)).sortByKey(false).take(10)
  mostCommon.foreach(println)

}
