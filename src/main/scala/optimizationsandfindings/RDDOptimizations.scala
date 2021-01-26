package optimizationsandfindings

import org.apache.spark.HashPartitioner
import org.apache.spark.sql.SparkSession

import scala.util.Random

object RDDOptimizations extends App {

  val spark = SparkSession.builder().appName("QueryPlans")
    .master("local")
    .getOrCreate()

  val sc = spark.sparkContext
  sc.setLogLevel("ERROR")

  val cmRDDLocation = "file:///mnt/d/findataf/201819/cm"
  val cmRDD = sc.textFile(cmRDDLocation)
  val cmRDDMapped = cmRDD.
    map(_ split ",").
    filter(x => x(0) != "SYMBOL").
    filter(x => x(1) == "EQ").
    map(x => (x(0), x(5), x(8), x(10)))

  cmRDDMapped.
    take(5).
    foreach(println)

  def transformCMDate(dt: String): String = {
    val calendar = Map[String, String]("JAN" -> "01", "FEB" -> "02", "MAR" -> "03", "APR" -> "04",
      "MAY" -> "05", "JUN" -> "06", "JUL" -> "07", "AUG" -> "08", "SEP" -> "09", "OCT" -> "10",
      "NOV" -> "11", "DEC" -> "12")
    dt.substring(dt.length - 4, dt.length) + "-" +
      calendar(dt.toUpperCase.substring(3, 6)) + "-" +
      dt.substring(0, 2)
  }

  val cmRDDKeyedBySymbol = cmRDDMapped.map {
    case (symbol, closepr, qty, date) => (symbol, (transformCMDate(date), closepr, qty))
  }

  val prvolLocation = "file:///mnt/d/findataf/201819/prvolmod"
  val prvolRDD = sc.textFile(prvolLocation)

  val prvolRDDKeyedBySymbol = prvolRDD.map(_ split ",").
    filter(x => x(3) == "EQ").
    map {
      x => (x(2), (x(7), x(6)))
    }

  cmRDDKeyedBySymbol.join(
    prvolRDDKeyedBySymbol).
    filter(x => x._2._1._3.toInt > 10000000 && x._2._2._2.toDouble >= 60.0).
    reduceByKey((x, y) => if (x._2._2.toDouble > y._2._2.toDouble) x else y)

  cmRDDKeyedBySymbol.join(
    prvolRDDKeyedBySymbol).
    filter(x => x._2._1._3.toInt > 10000000 && x._2._2._2.toDouble >= 60.0).
    reduceByKey((x, y) => if (x._2._2.toDouble > y._2._2.toDouble) x else y).
    filter(_._1 == "ITC").collect

  cmRDDKeyedBySymbol.join(
    prvolRDDKeyedBySymbol).
    filter(x => x._2._1._3.toInt > 10000000 && x._2._2._2.toDouble >= 60.0).
    reduceByKey((x, y) => if (x._1._2.toDouble > y._1._2.toDouble) x else y).
    filter(_._1 == "ITC").collect

  cmRDDKeyedBySymbol.filter(x => x._2._3.toInt > 10000000).
    reduceByKey((x, y) => if (x._2.toDouble > y._2.toDouble) x else y).
    join(prvolRDDKeyedBySymbol.
      filter(x => x._2._2.toDouble >= 60.0).
      reduceByKey((x, y) => if (x._2.toDouble > y._2.toDouble) x else y)
    )

  cmRDDKeyedBySymbol.filter(x => x._2._3.toInt > 10000000).
    reduceByKey((x, y) => if (x._2.toDouble > y._2.toDouble) x else y).
    join(prvolRDDKeyedBySymbol.
      filter(x => x._2._2.toDouble >= 60.0).
      reduceByKey((x, y) => if (x._2.toDouble > y._2.toDouble) x else y)
    ).filter(_._1 == "ITC").collect

  // ##############################################################
  //  ##            Broadcasting with RDDs                        ##
  //  ##############################################################

  // Use broadcasting with RDDs
  // collect the value to use as a broadcast variable
  // often useful to collect as a map and use the map to find corresponding attributes
  val prvolBroadcastVal = prvolRDDKeyedBySymbol.
    filter(x => x._2._2.toDouble >= 60.0).
    reduceByKey((x, y) => if (x._2.toDouble > y._2.toDouble) x else y).collectAsMap()

  // use the broadcast map to enrich the values with date and maximum delivery percentage tuple
  cmRDDKeyedBySymbol.filter(x => x._2._3.toInt > 10000000).
    reduceByKey((x, y) => if (x._2.toDouble > y._2.toDouble) x else y).
    map(x => (x, prvolBroadcastVal.getOrElse(x._1, 0)))

  // verify using ITC
  cmRDDKeyedBySymbol.filter(x => x._2._3.toInt > 10000000).
    reduceByKey((x, y) => if (x._2.toDouble > y._2.toDouble) x else y).
    map(x => (x, prvolBroadcastVal.getOrElse(x._1, 0))).
    filter(_._1 == "ITC").collect

  // ##############################################################
  //  ##           Copartitioning RDDs                           ##
  //  ##############################################################
  val prvolRDDKeyedBySymbolPartitioned = prvolRDDKeyedBySymbol.
    partitionBy(new HashPartitioner(4))

  val coPartitionerForCM = prvolRDDKeyedBySymbolPartitioned.partitioner match {
    case None => new HashPartitioner(prvolRDDKeyedBySymbolPartitioned.getNumPartitions)
    case Some(partitoner) => partitoner
  }

  val cmRDDKeyedBySymbolPartitioned = cmRDDKeyedBySymbol.
    partitionBy(coPartitionerForCM)

  // verify that keys go the same numbered partition for both RDDs
  prvolRDDKeyedBySymbolPartitioned.glom.map(x => x.map(_._1)).collect()(0)
  cmRDDKeyedBySymbolPartitioned.glom.map(x => x.map(_._1)).collect()(0)

  val prvolKeysSet = prvolRDDKeyedBySymbolPartitioned.glom.map(x => x.map(_._1)).collect()(0).toSet
  val cmKeysSet = cmRDDKeyedBySymbolPartitioned.glom.map(x => x.map(_._1)).collect()(0).toSet
  cmKeysSet.filter(x => ! prvolKeysSet.contains(x))



  cmRDDKeyedBySymbolPartitioned.filter(x => x._2._3.toInt > 10000000).
    reduceByKey((x, y) => if (x._2.toDouble > y._2.toDouble) x else y).
    join(prvolRDDKeyedBySymbolPartitioned.
      filter(x => x._2._2.toDouble >= 60.0).
      reduceByKey((x, y) => if (x._2.toDouble > y._2.toDouble) x else y)
    )

  // ##############################################################
  //  ##           CoGrouping                                    ##
  //  ##############################################################

  val foDataLocation = "file:///mnt/d/findataf/201819/fo"
  val foRDD = sc.textFile(foDataLocation)

  val foRDDKeyedBySymbolAndDate = foRDD.map {
    x => x.split(",")
  }.
    filter(_ (1) != "SYMBOL").
    filter(_ (0).substring(0, 3) == "FUT").
    map {
      x => ((x(1), transformCMDate(x(14))), (x(10), x(11)))
    }
  foRDDKeyedBySymbolAndDate.take(5)
  val cmRDDKeyedBySymbolAndDate = cmRDDMapped.map {
    case (symbol, closepr, qty, date) => ((symbol, transformCMDate(date)), (closepr, qty))
  }

  val prvolRDDKeyedBySymbolAndDate = prvolRDD.map(_ split ",").
    filter(x => x(3) == "EQ").
    map {
      x => ((x(2), x(7)), x(6))
    }

  foRDDKeyedBySymbolAndDate.
    join(cmRDDKeyedBySymbolAndDate).
    join(prvolRDDKeyedBySymbolAndDate).
    filter(x => x._1._1 == "ITC" && x._1._2 == "2018-08-31")

  val foCMPrvCoGrpuped = foRDDKeyedBySymbolAndDate.
    cogroup(cmRDDKeyedBySymbolAndDate, prvolRDDKeyedBySymbolAndDate)

  foCMPrvCoGrpuped.reduceByKey {
    case (x, y) => if (x._3.toList(0).toDouble > y._3.toList(0).toDouble) x else y
  }

  foCMPrvCoGrpuped.reduceByKey {
    case (x, y) => if (x._3.toList(0).toDouble > y._3.toList(0).toDouble) x else y
  }.filter(x => x._1._1 == "ITC" && x._1._2 == "2018-08-31").collect

  // ##############################################################
  //  ##           Skewed RDD Join                               ##
  //  ##############################################################

  // salt the rdd with extra column
  cmRDDMapped.filter(_._1 == "ITC").
    flatMap(x => (0 to 9).
      map { _ =>
        val rndm = new Random()
        val rno = rndm.nextInt(10)
        ((x._1, x._4, rno), (x._2, x._3))
      })

  // explode the dimension with extra index column
  sc.parallelize(List(("ITC", "01-APR-2019", "someattr"))).
    flatMap(x => (0 to 9).map(
      sno => ((x._1, x._2, sno), x._3))).
    collect.foreach(println)
}
