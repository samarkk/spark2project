package com.skk.training.sparkcore

import org.apache.spark.sql.SparkSession
import scala.util.Random

object SparkTransformations {
  def main(args: Array[String]) {
    val spark = SparkSession.builder()
      .appName("SparkCoreTransformations")
      .master(args(0))
      .getOrCreate()

    //spark.conf.set("spark.local.dir", "D:\temp")
    val sc = spark.sparkContext
    //  println(sc.version + " , " + spark.version)

    sc.setLogLevel("ERROR")

    // Map
    // Map - RDD
    // def map[U](f: (T) ⇒ U)(implicit arg0: ClassTag[U]): RDD[U]
    val aColl = Array("a", "b", "c")
    val aCollRDD = sc parallelize aColl
    println((aCollRDD collect) mkString ",")
    println("Map transformation " + aCollRDD.map(x => (x, 1)).collect.mkString(","))

    //  val aCollDF = (sc parallelize aColl) toDF ("c1")
    //  aCollDF.printSchema()
    //  println(aCollDF.select("c1").collect().mkString(","))

    // filter - filter RDD
    // def filter(f: (T) ⇒ Boolean): RDD[T]
    val noRDD = sc parallelize (1 to 10)
    println("Filtering to only print odd numbers " + noRDD.filter(_ % 2 != 0).collect.mkString(" "))

    //flatMap - RDD
    // def flatMap[U](f: (T) ⇒ TraversableOnce[U])(implicit arg0: ClassTag[U]): RDD[U]
    val ottRDD = sc.parallelize(List(1, 2, 3))
    println("Flatmapping  illustration \n")
    println(ottRDD.flatMap(x => Array(x, x * 100, 42)).collect.mkString(","))

    // group by
    // def groupBy[K](f: (T) ⇒ K, p: Partitioner)(implicit kt: ClassTag[K], ord: Ordering[K] = null):
    // RDD[(K, Iterable[T])]
    println("groupBy Transformation:\n")
    noRDD.groupBy(x => x % 2).collect.foreach(println)
    noRDD.groupBy(_ % 2).map { x => { if (x._1 == 0) ("even", x._2) else ("odd", x._2) }
    }.collect.foreach(println)

    // groupByKey
    // def groupByKey(): RDD[(K, Iterable[V])]
    // How do rdd functions become available for pair rdds automatically
    // there is an implicit definition in the accompanying object
    //   implicit def rddToPairRDDFunctions[K, V](rdd: RDD[(K, V)])
    //    (implicit kt: ClassTag[K], vt: ClassTag[V], ord: Ordering[K] = null): PairRDDFunctions[K, V] = {
    //    new PairRDDFunctions(rdd)
    //  }
    println("/ngroupByKey transformation")
    val pow1t3RDD = sc.parallelize(List((1, 1), (1, 1), (1, 1), (2, 2), (2, 4), (2, 8), (3, 3), (3, 9), (3, 27)))
    pow1t3RDD.groupByKey.collect.foreach(println)
    // we can import and create PairRDDFunctions explicitly
    println("/ngroupByKey transformation using explicit declarations")
    import org.apache.spark.rdd.PairRDDFunctions
    val demoPRDF = new PairRDDFunctions(pow1t3RDD)

    //reduceByKey
    // def reduceByKey(partitioner: Partitioner, func: (V, V) ⇒ V): RDD[(K, V)]
    println("reduceByKey: " + pow1t3RDD.reduceByKey(_ + _).collect.mkString(","))

    // groupByKey vs reduceByKey
    println("groupByKey leading to reduceByKey: " +
      pow1t3RDD.groupByKey.map { case (no, iterNo) => (no, iterNo.sum) }.collect.mkString(","))

    //mapPartitions
    // def mapPartitions[U](f: (Iterator[T]) ⇒ Iterator[U], preservesPartitioning: Boolean = false)(implicit arg0: ClassTag[U]): RDD[U]
    // def glom(): RDD[Array[T]]
    // glom Returns an RDD created by coalescing all elements within each partition into an array
    val x = sc.parallelize(Array(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12), 3)
    x.mapPartitions(x => (x.sum, 42).productIterator).glom().collect

    //mapPartitionsWithIndex
    // def mapPartitionsWithIndex[U](f: (Int, Iterator[T]) ⇒ Iterator[U],
    // preservesPartitioning: Boolean = false)(implicit arg0: ClassTag[U]): RDD[U]
    println("Map partitions with index:")
    println(x.mapPartitionsWithIndex((x, iter) => iter.map(pelem => (x, pelem))).collect.mkString(","))
    def f(x: Int, iter: Iterator[Int]) = iter.map(pelem => (x, pelem))
    println(x.mapPartitionsWithIndex(f).collect.mkString(","))

    // sample
    // def sample(withReplacement: Boolean, fraction: Double, seed: Long = Utils.random.nextLong): RDD[T]
    println("sample illustration: " + x.sample(false, 0.4).collect.mkString(","))

    //union
    // def union(other: RDD[T]): RDD[T]
    val xu = sc.parallelize(List(1, 2, 3), 2)
    val yu = sc.parallelize(List(3, 4), 1)
    println("union: " + xu.union(yu).collect.mkString(","))
    println("union glom: " + xu.union(yu).glom.collect.mkString(","))

    // join
    // def join[W](other: RDD[(K, W)], numPartitions: Int): RDD[(K, (V, W))]
    // println join - by default inner join
    val xj = sc.parallelize(List(("a", 1), ("b", 2)))
    val yj = sc.parallelize(List(("a", 3), ("a", 4), ("b", 5)))
    println(xj.join(yj).collect.mkString(","))

    // leftOuterJoin and rightOuterJoin
    // def leftOuterJoin[W](other: RDD[(K, W)], numPartitions: Int): RDD[(K, (V, Option[W]))]
    // def rightOuterJoin[W](other: RDD[(K, W)], numPartitions: Int): RDD[(K, (Option[V], W))]
    println("Left Outer Join")
    val xoj = sc.parallelize(List(("a", 1), ("b", 2), ("c", 7)))
    val yoj = sc.parallelize(List(("a", 3), ("a", 4), ("b", 5), ("d", 4)))
    println(xoj.leftOuterJoin(yoj).collect.mkString(","))
    println("Right Outer Join")
    println(xoj.rightOuterJoin(yoj).collect.mkString(","))

    // fullOuterJoin
    // def fullOuterJoin[W](other: RDD[(K, W)], numPartitions: Int): RDD[(K, (Option[V], Option[W]))]
    println("Full Outer Join")
    println(xoj.fullOuterJoin(yoj).collect.mkString(","))

    // distinct
    // def distinct(): RDD[T]
    println("Distinct:")
    val aseq = for (n <- 1 to 10) yield scala.util.Random.nextInt(5)
    println(sc.parallelize(aseq).distinct.collect.mkString(","))

    //coalesce
    // def coalesce(numPartitions: Int, shuffle: Boolean =
    // false)(implicit ord: Ordering[T] = null): RDD[T]
    println("coalesce to reduce number of partitions")
    val xCoalesce = sc.parallelize(1 to 10, 4)
    println(xCoalesce.getNumPartitions)
    val yCoalesce = xCoalesce.coalesce(2)
    println(yCoalesce.getNumPartitions)

    // keyBy
    // def keyBy[K](f: (T) ⇒ K): RDD[(K, T)]
    println("Key By")
    println(noRDD.keyBy(_ % 2 == 0).map(
      x => if (x._1) ("even", x._2) else ("odd", x._2)).collect.mkString(","))

    //partitionBy
    //def partitionBy(partitioner: Partitioner): RDD[(K, V)]
    val x_part = sc.parallelize(Array(('J', "James"), ('F', "Fred"), ('A', "Anna"), ('J', "John")), 3)
    import org.apache.spark.Partitioner
    val y_part = x_part.partitionBy(new Partitioner() {
      val numPartitions = 2
      def getPartition(k: Any): Int = if (k.asInstanceOf[Char] < 'H') 0 else 1
    })
    println("\nThe output post creation of the partitioner : ")
    y_part.glom.collect().foreach(x => println(x.mkString(",")))
    println("Verifying the partitions created post the partitioner")
    y_part.mapPartitionsWithIndex((idx, iter) => {
      iter.toList.map(x => (idx, x)).iterator
    }).collect.foreach(println)

    // zip and zipPartitions
    // def zip[U](other: RDD[U])(implicit arg0: ClassTag[U]): RDD[(T, U)]
    // def zipPartitions[B, V](rdd2: RDD[B])(f: (Iterator[T], Iterator[B])
    // ⇒ Iterator[V])(implicit arg0: ClassTag[B], arg1: ClassTag[V]): RDD[V]
    val azip = sc.parallelize(1 to 4)
    val bzip = sc.parallelize(List(1, 4, 9, 16))
    val czip = sc.parallelize(List(1, 8, 27))
    println("Zipping RDDs")
    println(azip.zip(bzip).collect.mkString(","))
    def zipfunc(itera: Iterator[Int], iterb: Iterator[Int]): Iterator[String] = {
      var res = List[String]()
      while (itera.hasNext && iterb.hasNext) { val x = itera.next + " " + iterb.next; res ::= x }
      res.iterator
    }
    println("Zipping RDD Partitions")
    println(azip.zipPartitions(bzip)(zipfunc).collect.mkString(","))
    println("Zipping RDD Partitions to produce sum of elements in the partitions")
    println(azip.zipPartitions(bzip)((ia, ib) => {
      var res = List[Int](); while (ia.hasNext && ib.hasNext) res ::= ia.next + ib.next
      res.iterator
    }).collect.mkString(","))

    // sortBy
    // def sortBy[K](f: (T) ⇒ K, ascending: Boolean = true, numPartitions: Int = this.partitions.length
    // )(implicit ord: Ordering[K], ctag: ClassTag[K]): RDD[T]
    val rand = new scala.util.Random
    val randRDD = sc.parallelize(for (n <- 1 to 10) yield rand.nextInt(20))
    println("Simple RDD sort ascending")
    println(randRDD.sortBy(x => x).collect.mkString(","))
    println("Simple RDD sort descending")
    println(randRDD.sortBy(x => -x).collect.mkString(","))
    // complex rdd sort by multiple fields different orders
    println("Complex RDD sort using Ordering and ClassTag explicitly")
    val compRDD = sc.parallelize(List(
      ("arjun", "tendulkar", 5),
      ("sachin", "tendulkar", 102), ("vachin", "tendulkar", 102),
      ("rahul", "dravid", 74), ("vahul", "dravid", 74),
      ("rahul", "shavid", 74), ("vahul", "shavid", 74),
      ("jacques", "kallis", 92), ("ricky", "ponting", 84), ("jacques", "zaalim", 92),
      ("sachin", "vendulkar", 102)))
    val cust_ord: Ordering[(String, String, Int)] =
      Ordering.Tuple3(Ordering.String.reverse, Ordering.String, Ordering.Int)
    import scala.reflect.classTag
    val ctg = classTag[(String, String, Int)]
    // def sortBy[T](f: (T) => K)
    // so we can specify a function which will map from T to a type K that is
    // different from T but will return back to us T only
    // therefore to sort descending by middle name and then by first name
    // we have specified Ordering.String.reverse in the first position
    // and in the function that we use we put the second part - the middle name
    // in the first position
    compRDD.sortBy(x => (x._2, x._1, x._3))(cust_ord, ctg).collect.foreach(println)

    println("\nComplex RDD sort using implicits")
    implicit val cust_ord_impl: Ordering[(String, String, Int)] =
      Ordering.Tuple3(Ordering.String.reverse, Ordering.String, Ordering.Int)
    compRDD.sortBy(x => (x._2, x._1, x._3)).collect.foreach(println)

    //def takeOrdered(num: Int)(implicit ord: Ordering[T]): Array[T]
    println("using the implicit custom ordering in takeOrdered")
    compRDD.takeOrdered(2).foreach(println)

    // def aggregate[U](zeroValue: U)(seqOp: (U, T) ⇒ U, combOp: (U, U) ⇒ U)(
    // implicit arg0: ClassTag[U]): U
    /*
  Aggregate the elements of each partition, and then the results for all the partitions,
  using given combine functions and a neutral "zero value". This function can return
  a different result type, U, than the type of this RDD, T. Thus, we need one operation
  for merging a T into an U and one operation for merging two U's,
  as in scala.TraversableOnce. Both of these functions are allowed to modify and return
  their first argument instead of creating a new U to avoid memory allocation
  */
    println("\nThe aggregate operation where the returned result can be different from the RDD's type")
    def seqOp = (data: (Array[Int], Int), item: Int) => (data._1 :+ item, data._2 + item)
    def combOp = (d1: (Array[Int], Int), d2: (Array[Int], Int)) =>
      (d1._1.union(d2._1), d1._2 + d2._2)
    val xaggr = sc.parallelize(Array(1, 2, 3, 4))
    val yaggr = xaggr.aggregate((Array[Int](), 0))(seqOp, combOp)
    println(yaggr._1.mkString(",") + " , " + yaggr._2)

    println("\nUsing printlns inside aggregate to get the complete picture on aggregate")
    val ardd = sc.parallelize(1 to 12, 6)
    println("\n First look at the data in the different partitions")
    ardd.mapPartitionsWithIndex((idx, iter) => iter.toList.map((idx, _)).iterator).collect.foreach(println)
    ardd.aggregate(0, 0)(
      (acc, value) => {
        println("acc is " + acc + " and value is " + value)
        (acc._1 + value, acc._2 + 1)
      },
      (x, y) => {
        println("x is " + x + " and y is " + y)
        (x._1 + y._1, x._2 + y._2)
      })
    /*
     To undrestand look at the data assigned to different partitions
     eg - for 1 to 12, we have six partitions and then we have 0,1 in 0, 2,3 in 2, 4,5 in 3 and so on
     Then look at how the aggregation is taking place
     eg we will have acc is (0,0) and value is 1
     eg the seqop will create (1,1) and we should look for acc is (1,1) and value is 2
     the seqop will thus create (1+2, 1+1) i.e (3,2) and this will be used in the combop
     one more - for partition no 2 i.e indexed 1 we will have
     acc is (0,0) and value is 3 which will create ( 0+3, 0+1) and we will have
     acc is  (3,1) and value is 4 which should lead to (3+4, 1+1) ie. (7,2) which goes to combop
     thus partitions six - we will have six pairs - each having 2 as the second part of the tuple
     */
    println("\nNow looking at how the tree aggregate is going to work")
    ardd.treeAggregate(0, 0)(
      (acc, value) => {
        println("acc is " + acc + " and value is " + value)
        (acc._1 + value, acc._2 + 1)
      },
      (x, y) => {
        println("x is" + x + "and y is " + y)
        (x._1 + y._1, x._2 + y._2)
      })
    /*
     * Tree aggregations operate exactly in the same way as aggreate
     * except for one critical difference - there is an intermediate aggregation step
     * data from some partitions will be sent to executors to aggregate
     * so in the above case if there are six partitions
     * while aggregate will send results of all the six partitions to the driver
     * in tree aggregate, three will go to one executor, three to another
     * and the driver will receive the aggregations from 2 rather than 6
     * where there are many number of partitions tree aggregate performs significantly better than
     * vanilla aggregate
     */

    // combineByKey
    // def combineByKey[C](createCombiner: (V) ⇒ C, mergeValue: (C, V) ⇒ C,
    // mergeCombiners: (C, C) ⇒ C): RDD[(K, C)]
    println("\nCombine by key, create a combiner, then merge a value into " +
      "the list, and then merge the combiners")
    val frd = sc.parallelize(
      List((2, 4), (2, 8), (3, 9), (3, 27), (3, 81),
        (2, 16), (2, 32), (2, 64), (2, 128), (2, 256),
        (3, 243), (3, 729), (5, 625), (5, 3125), (5, 15625),
        (3, 2187), (3, 6501), (5, 25), (5, 125), (5, 78125), (5, 390625)))

    println("\nTake a look at the allocation across partitions")
    frd.mapPartitionsWithIndex(
      (x, iter) => iter.toList.map(a => (x, a)).iterator).collect.foreach(println)

    val frdCmbVal = frd.combineByKey(
      x => {
        println("x is " + x)
        (x, 1)
      },
      (acc: (Int, Int), vlu: Int) =>
        {
          println("acc is " + acc + " and vlu is " + vlu)
          (acc._1 + vlu, acc._2 + 1)
        },
      (a: (Int, Int), b: (Int, Int)) => {
        println("a is " + a + " and b is " + b)
        (a._1 + b._1, a._2 + b._2)
      })

    //  frdCmbVal.collect().foreach(println)
    // Now take a look at the steps for key 3
    // first time 3 is encountered in a partition, the step (v) => C
    // the combiner is created
    // next time 3 is there in a partitioner the merge value step takes place
    // so if we have 9 as the first value, the combiner (9, 1) is created
    // when 27 is encountered the merge value leads to (9 + 27, 1 + 1) i.e (36, 2)
    // we have this repeated for each partition
    // so we should get (117, 3), (972, 2) and (8688, 2) which give us (3, (9777, 7))
    println("And the collected values from combine by key are: " + frdCmbVal.collect.mkString(","))

    // aggregateByKey
    // def aggregateByKey[U](zeroValue: U)(seqOp: (U, V) ⇒ U,
    // combOp: (U, U) ⇒ U)(implicit arg0: ClassTag[U]): RDD[(K, U)]
    println("Aggregate by key example")
    frd.aggregateByKey(0)(_ + _, _ + _).collect.foreach(println)

    val cmplxRDD = sc.parallelize(List(
      ("sachin", ("india", 200, 34)),
      ("sachin", ("wi", 24, 10)),
      ("rahul", ("india", 195, 29)), ("rahul", ("wi", 23, 11)),
      ("sachin", ("india", 215, 45)), ("rahul", ("india", 188, 17))))

    val cmpn = cmplxRDD.map {
      case (player, (country, matches, centuries)) =>
        ((player, country), (matches, centuries))
    }
    // this is as name implies merging values , later on in combineByKey we do (x: (Int, Int)) => (x, 1)
    // so the combiner is ((Int, Int), Int)
    // and we have to for a partition merge the values beginning with the first element for which we create the accumulator
    // and all of this is going to happen for each key - that is for each key within each partition
    // so first time it sees ("sachin", "india") it is going to create ((200, 340), 1)
    // and for the next value mrgval will come into plau and we should get ((200 + 215, 34 + 45), 1 + 1)
    def mrgval(x: ((Int, Int), Int), y: (Int, Int)) =
      ((x._1._1 + y._1, x._1._2 + y._2), x._2 + 1)
    // after the combiners have marged the values it is time to merge them
    def mrgcmb(x: ((Int, Int), Int), y: ((Int, Int), Int)) =
      ((x._1._1 + y._1._1, x._1._2 + y._1._2), x._2 + y._2)

    cmpn.combineByKey((x: (Int, Int)) => (x, 1), mrgval, mrgcmb).collect.foreach(println)

    // simpler way of doing stuff
    cmpn.combineByKey(
      x => x,
      (x: (Int, Int), y: (Int, Int)) => (x._1 + y._1, x._2 + y._2),
      (x: (Int, Int), y: (Int, Int)) => (x._1 + y._1, x._2 + y._2)).collect.foreach(println)

    cmpn.map(x => (x._1, (x._2._1, x._2._2, 1))).combineByKey(
      x => x,
      (x: (Int, Int, Int), y: (Int, Int, Int)) => (x._1 + y._1, x._2 + y._2, x._3 + y._3),
      (x: (Int, Int, Int), y: (Int, Int, Int)) =>
        (x._1 + y._1, x._2 + y._2, x._3 + y._3)).collect.foreach(println)

    import scala.util.Random
    val arand = new Random
    val countryList = List("India", "Austraila", "England", "West Indies",
      "Pakistan", "Bangladesh", "Newzealand", "South Africa", "Sri Lanka",
      "Zimbawe")
    val countryListLength = countryList.length

    val playerList = for (x <- 1 to 100000) yield {
      val idx = arand.nextInt(100)
      ("plr" + idx, (2009 + arand.nextInt(10), arand.nextInt(200), arand.nextInt(5)))
    }

    val playerListReplicated = List.fill(10)(playerList).flatMap(x => x)
    val playerRDD = sc.parallelize(playerListReplicated)

    val playerYearRunsCatchesRDD = playerRDD.mapPartitions(iter => iter.toList.map {
      case (player, (year, runs, catches)) => ((player, year), (runs, catches))
    }.groupBy(_._1).map(
      x => (x._1, x._2.map(_._2).reduce(
        (a, b) =>
          (a._1 + b._1, a._2 + b._2)))).iterator).
      reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))

    playerYearRunsCatchesRDD.collect.foreach(println)

    val playerCountryYearEarnings = for (x <- 1 to 1000; y <- 2009 to 2018) yield {
      (("plr" + arand.nextInt(100), countryList(arand.nextInt(10)), y,
        10 + 50 * arand.nextDouble))
    }

    val playerCYERDD = sc.parallelize(playerCountryYearEarnings)

    // get total runs, matches, earnings by player and year
    // first join the two rdds and then carry out the reduce operation
    playerRDD.map(x => ((x._1, x._2._1), (x._2._2, x._2._3)))
      .join(playerCYERDD.map(x => ((x._1, x._3), (x._4))))
      .reduceByKey((x, y) => ((x._1._1 + y._1._1, x._1._2 + y._1._2), x._2 + y._2))
      .collect.foreach(println)

    // reduce the two rdds first and then join
    playerRDD.map(x => ((x._1, x._2._1), (x._2._2, x._2._3)))
      .reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))
      .join(playerCYERDD
        .map(x => ((x._1, x._3), (x._4)))
        .reduceByKey(_ + _)).collect.foreach(println)

    // try with the rdd created using map partitions and check
    playerYearRunsCatchesRDD.join(
      playerCYERDD.map(x => ((x._1, x._3), (x._4))).reduceByKey(_ + _)).collect.foreach(println)

    // use a broadcast operation to reduce shuffle
    val playerYERReduced = playerCYERDD.map(x => ((x._1, x._3), (x._4))).reduceByKey(_ + _)
    val playerYERBroadcast = sc.broadcast(playerYERReduced.collectAsMap)
    val plyrb = playerYERBroadcast.value
    // a join operation is avoided here - it is transformed into a map
    playerRDD.map(x => ((x._1, x._2._1), (x._2._2, x._2._3, plyrb.getOrElse((x._1, x._2._1), 0))))
    //      .join(playerYERBroadcast.value)
    //      .reduceByKey((x, y) => ((x._1._1 + y._1._1, x._1._2 + y._1._2), x._2 + y._2))
    //      .collect.foreach(println)

    // handling skew - salting key
    val playerRDDSaltedForKeyPlr1 = playerRDD.filter(_._1 == "plr1").map { x =>
      {
        import scala.util.Random
        val arand = new Random
        val intForSaltedKey = 101 + arand.nextInt(10)
        ("plr" + intForSaltedKey, x._2)
      }
    }

    val playerRuns = playerRDDSaltedForKeyPlr1.map(x => (x._1, x._2._2)).reduceByKey(_ + _)
    val playerRunsSaltedKeys = playerRuns.filter(x => {
      val saltedKeys = (101 to 110).map("plr" + _)
      saltedKeys.contains(x._1)
    })
    val playerRunsUnsaltedKeysReduced = playerRuns.subtractByKey(playerRunsSaltedKeys)
    val playerSaltedKeysReduced = playerRunsSaltedKeys.reduceByKey(_ + _)
    val playerRunsReduced = playerRunsUnsaltedKeysReduced union playerSaltedKeysReduced

    val playerRunsForPlr1FromPlayerRDD =
      playerRDD
        .filter(_._1 == "plr1")
        .map(x => (x._1, x._2._2))
        .map(_._2).sum

    println("player runs for plr1 from salted keys: " + playerSaltedKeysReduced.map(_._2).sum +
      " and player runs for plr1 from playerRDD " + playerRunsForPlr1FromPlayerRDD)

    assert(
      playerSaltedKeysReduced.map(_._2).sum == playerRunsForPlr1FromPlayerRDD,
      "salted operation has some problem, check your workings")

  }

}
