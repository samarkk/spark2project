package optimizationsandfindings

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.desc


object Holder extends Serializable {
  @transient lazy val log: Logger = Logger.getLogger(getClass.getName)
}

object WindowSkewAndLogger {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("WindowSkewHandlerAndLogger")
      .master("local")
      .getOrCreate()

    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    // val cmdfSkewedSaveLocation = "file:///mnt/d/tmp/cmskew"
    val cmdfSkewedSaveLocation = args(0)
    val cmdfSkewed = spark.read.parquet(cmdfSkewedSaveLocation)
    // #####################################################################
    // ##########    Salted Window Function - RDD zipWithIndex Solution ####
    // #####################################################################

    // take a look at the data
    // due to some reason
    cmdfSkewed.selectExpr("*", "cast(qty as float) as qtyfl").orderBy(desc("qtyfl")).show
    val cmdfSkewedCount = cmdfSkewed.count - 1.0

    // we need to find out the percentile / percent rank
    /*
   need total sort across whatever measure we want to build the percentiles on
   we can try with monotonically increasing id - but they are not consecutive
   we have percent rank formula as rank for the row based on the measure / total number of ranks - 1
   the monotonically increasing ids are not sequential
   */

    cmdfSkewed.selectExpr("*", "cast(qty as float) as qtyfl").orderBy("qtyfl").selectExpr("*", "monotonically_increasing_id() as id").orderBy(desc("id")).show()

    /*
   we can sort the dataframe
   and convert to rdd and add on the required functionality as a calculation
   */
    cmdfSkewed.selectExpr("*", "cast(qty as float) as qtyfl").orderBy("qtyfl").rdd.zipWithIndex.map {
      case (row, idx) => (row.getAs[String]("symbol"), row.getAs[String]("tsp"), row.getAs[String]("qty"), row.getAs[String]("vlu"), row.getAs[Float]("qtyfl"), idx, idx / cmdfSkewedCount)
    }.sortBy {
      // symbol, timestamp, quantity, value, quantityFloat, index, percent rank
      case (symbol, tsp, qty, vlu, qtyfl, idx, p_rank) => -p_rank
    }.take(10).foreach(println)

    /*
  supposing we were asked to find out the max percentile hit for quantity by any symbol
  we can do a reduce by key
   */
    cmdfSkewed.selectExpr("*", "cast(qty as float) as qtyfl").orderBy("qtyfl").rdd.zipWithIndex.map {
      case (row, idx) => (row.getAs[String]("symbol"), (idx, idx / cmdfSkewedCount))
    }.reduceByKey((x, y) => if (x._2 < y._2) y else x).take(10).foreach(println)

    /*
   find the top 100 percentile ranks
   in this case, we can optimize and find them in the mapPartitions
   and restrict ourselves to 100 per partition
   and then do a repartition into 1 and
   apply the function again
   */
    // IteratorToIteratorTransformations
    def useI2ITransforms(cmdfSkewedCount: Double): Unit = {
      val TOP_PRs = 100
      val findMaxRankByPartition = (records: Iterator[(String, Double)]) => {
        implicit val ordering: Ordering[(String, Double)] = Ordering.by[(String, Double), Double](x => -x._2)
        // percentRankTreeSet
        val prTreeSet = new scala.collection.mutable.TreeSet[(String, Double)]()
        records.foreach {
          record =>
            //            Holder.log.info(s"adding $record to the treeset")
            prTreeSet.add(record)
            if (prTreeSet.size > TOP_PRs) {
              prTreeSet.remove(prTreeSet.last)
            }
          //            Holder.log.info(s"Current tree set ${prTreeSet.mkString(",")}")
        }
        prTreeSet.toIterator
      }
      cmdfSkewed.selectExpr("*", "cast(qty as float) as qtyfl").orderBy("qtyfl").rdd.zipWithIndex.
        map {
          case (row, idx) => (row.getAs[String]("symbol"), idx / cmdfSkewedCount)
        }.
        mapPartitions(findMaxRankByPartition).
        repartition(1).
        mapPartitions(findMaxRankByPartition).
        collect().foreach(println)
    }

    useI2ITransforms(cmdfSkewedCount)

    // #############################################################
    // ###  check the sql for overall percentile generation  #######
    // #############################################################

    // overall shuffle data written here is 250.3 mb
    // and time per task is 5 s for the shuffle and gc times are 0.3s and 0.2s
    // and shuffle read is 250.3 mb and while reading there is spill memory of 736mb and spill disk of 320.8 mb

    cmdfSkewed.createOrReplaceTempView("cmsktbl")
    spark.sql(
      """
        select symbol, sksym, tsp, qty, vlu,
        percent_rank() over (order by cast(qty as float)) as globalPercentile
        from cmsktbl
        order by globalPercentile desc
    """
    ).show(200, false)

    // Find percentiles by symbol

    // we have one task here taking a long time and a large shuffle and spill
    // which will be for the skewed symbol

    // use this query later on to verify the results against the circuitous RDD calculations
    spark.sql(
      """
        select symbol, sksym, tsp, qty, vlu,
        percent_rank() over (partition by sksym order by cast(qty as float))
        as symPercentile
        from cmsktbl
        where sksym = "AXISBANK"
        order by sksym, symPercentile desc
    """
    ).show(20, false)

    // ######################################################################
    // #####  process the skewed symbols separately #########################
    // #####################################################################
    val sksymCount = cmdfSkewed.filter("sksym = 'CMSYS'").count
    cmdfSkewed.filter("sksym = 'CMSYS'").selectExpr("*", "cast(qty as float) as qtyfl").orderBy("sksym", "qtyfl").rdd.zipWithIndex.map {
      case (row, idx) => (row.getAs[String]("symbol"), row.getAs[String]("sksym"), row.getAs[String]("tsp"), row.getAs[String]("qty"), row.getAs[String]("vlu"), row.getAs[Float]("qtyfl"), idx, idx.toDouble / (sksymCount - 1))
    }.sortBy {
      // symbol, timestamp, quantity, value, quantityFloat, index, percent rank
      case (symbol, sksym, tsp, qty, vlu, qtyfl, idx, p_rank) => -p_rank
    }.take(10).foreach(println)

    // #####################################################################
    // ##########  Circuitous RDD solution                  ################
    // #####################################################################

    // find minimum indices by symbol
    val minIndicesBySymbol = cmdfSkewed.selectExpr("*", "cast(qty as float) as qtyfl").orderBy("sksym", "qtyfl").rdd.zipWithIndex.map {
      case (row, idx) => (row.getAs[String]("sksym"), idx)
    }.reduceByKey((x, y) => if (x < y) x else y).cache

    // find maximum indices by symbol
    val maxIndicesBySymbol = cmdfSkewed.selectExpr("*", "cast(qty as float) as qtyfl").orderBy("sksym", "qtyfl").rdd.zipWithIndex.map {
      case (row, idx) => (row.getAs[String]("sksym"), idx)
    }.reduceByKey((x, y) => if (x < y) y else x).cache

    // join the two to find the beginning index for each symbol and the total ranks
    // and collect as a map
    val totalRanksBySymbolMap = maxIndicesBySymbol.join(minIndicesBySymbol).map {
      case (symbol, (maxidx, minidx)) => (symbol, (minidx, maxidx - minidx - 1))
    }.collectAsMap

    // broadcast total ranks as amap
    val symbolIdxBroadcast = sc.broadcast(totalRanksBySymbolMap)

    // subtract the beginning index from each index for the symobl
    // and divide it by the total ranks for the symbol
    // here we verify for one symbol - verify it agains the sql query to find ranks by synbol
    cmdfSkewed.selectExpr("*", "cast(qty as float) as qtyfl").orderBy("sksym", "qtyfl").rdd.zipWithIndex.map {
      case (row, idx) => (row.getAs[String]("symbol"), row.getAs[String]("tsp"), row.getAs[String]("qty"), row.getAs[String]("vlu"), row.getAs[Float]("qtyfl"), idx,
        (idx.toDouble - 1 - symbolIdxBroadcast.value.getOrElse(row.getAs[String]("sksym"), (1L, 1L))._1) / symbolIdxBroadcast.value.getOrElse(row.getAs[String]("sksym"), (1L, 1L))._2)
    }.filter(_._1 == "AXISBANK").sortBy(-_._7).take(20).foreach(println)

    Thread.sleep(10000)
  }
}
