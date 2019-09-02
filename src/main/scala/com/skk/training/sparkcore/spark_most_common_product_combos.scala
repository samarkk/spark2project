val order_items = spark.sql("select * from order_items")
val products = spark.sql("select * from products")

val orders_products = order_items.rdd.map { x => (
    x.getAs[Int]("order_item_product_id"),
    (x.getAs[Int]("order_item_order_id"), x.getAs[Int]("order_item_quantity")))
}.join(
  products.rdd.map { x => (
    x.getAs[Int]("product_id"),
    (x.getAs[String]("product_name")))
  }
)
spark.conf.set("spark.sql.crossJoin.enabled", true)
orders_products.take(10).foreach(println)

orders_products.map{
case (pid, ((orderid, orderquantity), productname)) => (orderid, (orderquantity, productname))
}.take(5).foreach(println)

val orders = orders_products.map{
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
            order_pair(0)._1 * order_pair(1)._1
        )
    )
  )
)

val combos = cooccurrences.flatMap(x => x._2).reduceByKey((a, b) => a + b)
val mostCommon = combos.map(x => (x._2, x._1)).sortByKey(false).take(10)
mostCommon.foreach(println)

