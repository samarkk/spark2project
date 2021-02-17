package optimizationsandfindings

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._


object SparkDFOptimizations extends App {
  val spark = SparkSession.builder().appName("SparkOptimizations")
    .master("local")
    .getOrCreate()

  val sc = spark.sparkContext
  sc.setLogLevel("ERROR")

  import spark.implicits._


  val fileLoc = "file:///mnt/d/findataf/cm/cmcsv"
  val prvolSchema = StructType(
    Array(
      StructField("rectype", StringType),
      StructField("srno", IntegerType),
      StructField("symbol", StringType),
      StructField("series", StringType),
      StructField("traded", IntegerType),
      StructField("deliverable", IntegerType),
      StructField("delper", DoubleType),
      StructField("trdate", TimestampType)
    )
  )
  // prvolmod has price volume data with the date from the filename appended
  // at the end
  val prVolDirLocation = "file:///mnt/d/findataf/prvol/prvolmod"
  val prvolDF = spark.
    read.
    schema(prvolSchema).
    option("dateformat", "dd-mm-yyyyy").
    csv(prVolDirLocation)


  // number of partitions of prvolDF
  // spark.files.openCostInBytes = 4MB
  // 2987 *4 +205  = 12153
  // 12153 / 128 = 94

  // stored as parquet -  size is 57M
  // if we do not repartition we will have 94 partitions
  val prvolDFSaveLocation = "file:///mnt/d/tmp/prvoldf"
  prvolDF.
    filter("Series = 'EQ'").
    repartition(1).
    write.
    mode("overwrite").
    parquet(prvolDFSaveLocation)

  // load the price volume dataframe from disk

  val prvolDFLocation = "file:///mnt/d/tmp/prvoldf"
  val prvolDFFmDisk = spark.read.parquet(prvolDFLocation)

  def createRandomUpperLower(symbol: String) = {
    val random = new scala.util.Random()
    val symbolLength = symbol.length
    val randomNumber = random.nextInt(3)
    val symbolCharToLower = random.nextInt(symbolLength)

    symbol.slice(0, symbolCharToLower) + symbol(symbolCharToLower).toLower + symbol.slice(symbolCharToLower + 1, symbolLength) + randomNumber

  }

  for (x <- 1 to 10) yield createRandomUpperLower("TCS")

  // set the location for the cash market data
  val cmDataLocation = "file:///mnt/d/findataf/cm/cmcsv"

  // read  the cash market data source
  // set infer schema to true and header to true
  // infer schema will scan 1 to 2 %  of each column to establish the data type for the column
  // for a large table providing the schema will save this overhead
  val cm_df = spark.
    read.
    option("inferSchema", value = true).
    option("header", value = true).
    csv(cmDataLocation)

  // drop the extra column inferred due to the trailing comma
  val cmdf = cm_df.drop("_c13")
  println("The cash market data frame schema")
  cmdf.printSchema
  val udfRandomUL = udf((symbol: String) => createRandomUpperLower(symbol))
  // register a name against
  spark.udf.register("udfrul", udfRandomUL)
  //  val cmdf = spark.read.option("inferSchema", true)
  //    .option()


  // create a function literal to replace monthnames with numbers
  // we want to transform 31-OCT-2019 to 31-10-2019
  // so we will create a map from JAN, FEB etc to 01, 02 etc
  // and use string replace to replace JAN with 01, FEB with 02 etc and son on
  val mnameToNo = (dt: String) => {
    val mname = dt.substring(3, 3 + 3)
    val calendar = Map[String, String]("JAN" -> "01", "FEB" -> "02", "MAR" -> "03", "APR" -> "04",
      "MAY" -> "05", "JUN" -> "06", "JUL" -> "07", "AUG" -> "08", "SEP" -> "09", "OCT" -> "10",
      "NOV" -> "11", "DEC" -> "12")
    // if it is in a proper date format anyway such as 2007-04-30 then the substring will be
    // 7-04 which will not be in the calendar
    // and we will take it as is using the None pattern match
    calendar.get(mname) match {
      case None => dt
      case Some(mn) => dt.substring(dt.length - 4, dt.length) + "-" + mn + "-" + dt.substring(0, 2)
    }
  }

  val udf_mname_to_no = udf(mnameToNo)
  spark.udf.register("umnametono", udf_mname_to_no)

  ////////////////////////////////////////////////////////////////////
  ///  Eliminaating functions, Broadcast Join                    ////
  ///////////////////////////////////////////////////////////////////

  // load the cash  market data frame
  // create cash market data frame with symbol upper lower random - symulr

  // add month and year to save it as a partitioned table
  // also while saving it reduce the number of partitions

  cmdf.
    filter("SERIES = 'EQ'").
    withColumn("symulr", udfRandomUL($"symbol")).
    withColumn("yr", year(
      to_timestamp(udf_mname_to_no($"TIMESTAMP"))
    )).
    withColumn("mnth", month(
      to_timestamp(udf_mname_to_no($"TIMESTAMP"))
    )).
    selectExpr(
      "symbol",
      "symulr",
      "series",
      "tottrdqty as qty",
      "tottrdval as vlu",
      "to_timestamp(umnametono(timestamp)) as tsp",
      "yr",
      "mnth",
      "TOTALTRADES as trades"
    ).
    show

  val cmdf4j = cmdf.
    filter("SERIES = 'EQ'").
    withColumn("symulr", udfRandomUL($"symbol")).
    withColumn("yr", year(
      to_timestamp(udf_mname_to_no($"TIMESTAMP"))
    )).
    withColumn("mnth", month(
      to_timestamp(udf_mname_to_no($"TIMESTAMP"))
    )).
    selectExpr(
      "symbol",
      "symulr",
      "series",
      "tottrdqty as qty",
      "tottrdval as vlu",
      "to_timestamp(umnametono(timestamp)) as tsp",
      "yr",
      "mnth",
      "TOTALTRADES as trades"
    )
  // lets  save cmdf4j as a  parquet dataframe with two partitions
  // while writing the data we can see warnings for data where
  // we have the totaltrades and isin column missing and values
  // for these columns will be null
  val savedCMDF4JLocation = "file:///mnt/d/tmp/cmdf_plain_df"
  cmdf4j.
    repartition(4).
    write.
    mode("overwrite").
    parquet(savedCMDF4JLocation)

  /*
    cmdf.selectExpr(
      "SERIES", "OPEN", "HIGH", "CLOSE", "LOW",
      "LAST", "ISIN", "TOTALTRADES",
      "TIMESTAMP", "PREVCLOSE", "TOTTRDQTY",
      "TOTTRDVAL", "SYMBOL").
      filter("TIMESTAMP = '01-JAN-2020'").
      repartition(1).
      write.
      option("header", value = true).
      csv("file:///mnt/d/tmp/csvshufflechk")

    val shuffledCSVDF = spark.read.option("inferSchema",true).
      option("header",true).
      csv("file:///mnt/d/tmp/csvshufflechk")
      shuffledCSVDF.printSchema()
    shuffledCSVDF.filter("TIMESTAMP = '01-JAN-2020'").show
    shuffledCSVDF.filter("TIMESTAMP = '02-JAN-2020'").show
    shuffledCSVDF.select("timestamp").distinct.count
    */

  /*
  repartition is for repartitioning in memory
  cmdf4j.
    repartition(2, $"tsp", $"symbol").
    write.
    parquet("cmdf_part_tsp_symbol_df")
   */

  // let us load back the data frame written to parquet
  // and create distinct symbol and symulr
  // and join with

  val cmdf4jFmDisk = spark.
    read.
    parquet(savedCMDF4JLocation)

  // with functions filter push down is eliminated
  val demdf = sc.parallelize(
    List(("Delhi", "ram"), ("delhi", "chire"), ("mumbai", "Rajnish"), ("Mumbai", "Vinay"))).
    toDF("city", "person")

  val demDFSaveLocation = "file:///mnt/d/tmp/demdf"
  demdf.write.mode("overwrite").save(demDFSaveLocation)

  // filters are pushed down to file system
  spark.read.parquet(demDFSaveLocation).filter("city = 'Delhi'").explain()
  // filter push down is eliminated with functions
  spark.read.parquet(demDFSaveLocation).filter("lower(city) = 'delhi'").explain()

  // we will create a table correlating mixed case symbols with the base symbol
  // and eliminate the need to use a function in the querying
  // which will ensure that the filters are pushed down to the base data

  // create symbol and linked symbol data frame
  // we should be able to see tCs3 is TCS,  TCS2 is TCS and so on

  val symldf = cmdf4jFmDisk.select("tsp", "symbol", "symulr").distinct.repartition(1)

  // small dataset - we can set shuffle to low no
  spark.conf.set("spark.sql.shuffle.partitions", 2)

  // write the symldf dataframe to disk
  symldf.write.parquet("symbol_ref_df")

  // load it back from the disk
  val symbolDFLocation = "hdfs://localhost:8020/user/samar/symbol_ref_df"
  val symdf = spark.read.parquet(symbolDFLocation)


  // join with prvol data frame
  // do a select distinct on cmdf and get the symbols altered
  prvolDFFmDisk.
    join(broadcast(symdf),
      prvolDFFmDisk.col("trdate") === symdf.col("tsp") &&
        prvolDFFmDisk.col("symbol") === symdf.col("symbol")
    )
  //  WARN TaskMemoryManager: Failed to allocate a page (268435456 bytes), try again
  prvolDFFmDisk.
    join(symdf,
      prvolDFFmDisk.col("trdate") === symdf.col("tsp") &&
        prvolDFFmDisk.col("symbol") === symdf.col("symbol")
    )
  val symbolToFind = "TCS"
  val symbolToFindDF = symdf.filter("symbol = '" + symbolToFind + "'")
  //  symbolToFindDF.show
  prvolDFFmDisk.
    join(symbolToFindDF,
      prvolDFFmDisk.col("trdate") === symbolToFindDF.col("tsp") &&
        prvolDFFmDisk.col("symbol") === symbolToFindDF.col("symbol")
    )
  prvolDFFmDisk.
    join(broadcast(symbolToFindDF),
      prvolDFFmDisk.col("trdate") === symbolToFindDF.col("tsp") &&
        prvolDFFmDisk.col("symbol") === symbolToFindDF.col("symbol")
    )

  // we can capture the functionality in a method and run multiple queries
  // each for one symbol or for a set of symbols
  def getRowsForSymbol(symbol: String, symdf: DataFrame,
                       prvoldf: DataFrame): DataFrame = {
    val symbolToFindDF = symdf.filter("symbol = '" + symbol + "'")
    prvoldf.
      join(broadcast(symbolToFindDF),
        prvoldf.col("trdate") === symbolToFindDF.col("tsp") &&
          prvoldf.col("symbol") === symbolToFindDF.col("symbol")
      )
  }

  for (symbol <- List("ACC", "HDFCBANK", "HDFC", "INFY", "TCS"))
    getRowsForSymbol(symbol, symdf, prvolDFFmDisk).show(1000, truncate = false)

  // with a table within the auto broadcaast join threshold broadcast will be piced up automatically
  val symdf1720 = symdf.filter("year(tsp) between 2017 and 2020")
  val symdflimLocation = "hdfs://localhost:8020/user/samar/symdflim"
  symdf1720.write.parquet(symdflimLocation)
  val symdflim = spark.read.parquet(symdflimLocation)

  // broadcast gets pciked up automatically
  prvolDFFmDisk.join(symdflim,
    prvolDFFmDisk.col("trdate") === symdflim.col("tsp") &&
      prvolDFFmDisk("symbol") === symdflim.col("symbol")).explain


  // join with cash market data also
  // broadcast gets pushed through
  prvolDFFmDisk.join(symdflim,
    prvolDFFmDisk.col("trdate") === symdflim.col("tsp") &&
      prvolDFFmDisk("symbol") === symdflim.col("symbol")).
    join(cmdf4jFmDisk,
      cmdf4jFmDisk.col("tsp") === symdflim.col("tsp") &&
        cmdf4jFmDisk("symbol") === symdflim.col("symbol"))

  //####################################################################
  //###        Replace Join by Window ?                            #####
  //####################################################################
  val cmdfPrUnion = cmdf4jFmDisk.selectExpr("symbol", "series", "tsp", "qty", "vlu",
    "trades", "yr", "mnth", "null as delper").
    union(prvolDFFmDisk.selectExpr("symbol", "series", "trdate as tsp",
      "null as qty", "null as vlu", "null as trades", "null as yr", "null as mnth", "delper"))

  cmdfPrUnion.createOrReplaceTempView("jtbl")

  spark.sql(
    """
    select f.* from
    (
        select symbol, series, tsp, qty, vlu, trades,
        yr, mnth, delper,
        lag(delper, 1) over (partition by symbol, tsp order by tsp) as delperf
        from jtbl
    ) f
    where f.delperf is not null
    """
  ).show

  def addCols[T](ds: Dataset[T], n: Int) = {
    val cols = (1 to n).map(x => s"id * $x as col$x")
    ds.selectExpr(("id" +: cols): _*)
  }

  val ds1 = spark.range(10)
  val df1 = ds1.selectExpr("id", "col1")
  val df2 = ds1.selectExpr("id", "col3")
  val dfUnioned = df1.selectExpr("id", "col1", "null as col3").
    union(df2.selectExpr("id", "null as col1", "col3"))
  dfUnioned.createOrReplaceTempView("wjchk")
  spark.sql(
    """
    select f.* from
   (
      select id, col1, col3,
      max(col1) over (partition by id order by id) as mc1,
      lag(col1,1) over (partition by id order by id) as lagc from wjchk
   )f
   where lagc is not null
  """).show

  //////////////////////////////////////////////////////////////////////
  ////         DPP Dynamic Partition Pruning                    ///////
  /////////////////////////////////////////////////////////////////////

  // Dynamic partition pruning - an optimization introduced with Spark 3
  // when we filter a fact table on an attribute other than the dimension attribute
  // the partitions are still pruned usinig a subquery broadcast

  val cmdf4dpplim = cmdf4jFmDisk.filter("yr > 2015")
  val cmdfPartLocation = "file:///mnt/d/tmp/cmdpffpartyym"
  val cmdfPart: Unit =
    cmdf4dpplim.write.mode("overwrite").partitionBy("tsp").parquet(cmdfPartLocation)

  // load back from disk
  // vlu is not a part of the dimension table
  // it is in the fact table
  // partitions of the fact table are pruned
  val cmdfPartFmDisk = spark.read.parquet(cmdfPartLocation)
  cmdfPartFmDisk.join(symdflim, Array("tsp", "symbol")).
    filter("vlu > 10000000 and tsp between '2018-01-01' and '2018-03-31'").
    groupBy(cmdfPartFmDisk.col("symbol"), cmdfPartFmDisk.col("symulr"), cmdfPartFmDisk.col("yr"), cmdfPartFmDisk.col("mnth")).
    agg(sum("vlu") as "tvlu").explain
  //  +- SubqueryBroadcast dynamicpruning#4037, 0, [tsp#698, symbol#699], [id=#5331]

  cmdfPartFmDisk.join(symdflim, Array("tsp", "symbol")).
    filter("vlu > 10000000 and tsp between '2018-01-01' and '2018-03-31'").
    groupBy(cmdfPartFmDisk.col("symbol"), cmdfPartFmDisk.col("symulr"), cmdfPartFmDisk.col("yr"), cmdfPartFmDisk.col("mnth")).
    agg(sum("vlu") as "tvlu").show

  /////////////////////////////////////////////////////////////////
  ////               Bucketing                                ////
  ///////////////////////////////////////////////////////////////

  // Bucketing will divide data into buckets
  // A particular key will go to the same bucket
  // Akin to pre partitioning for the dataframe
  // Grouping, Aggregation queries will benefit
  // joins will benefit if we bucket all the tables on the same column with the same number
  // of buckets

  val cmdfBucketTableName = "cmdfbucketed"
  cmdf4jFmDisk.write.mode("overwrite").bucketBy(32, "tsp", "symbol").saveAsTable(cmdfBucketTableName)
  val cmdfBucketed = spark.read.table(cmdfBucketTableName)
  cmdfBucketed.filter("tsp = '2019-04-23' and symbol = 'TCS'").explain
  // the exchange will disappear from the bucketed table
  cmdfBucketed.groupBy("tsp", "symbol").agg(sum("vlu") as "totvlu").explain
  cmdf4jFmDisk.groupBy("tsp", "symbol").agg(sum("vlu") as "totvlu").explain

  /////////////////////////////////////////////////////////////////////
  ////         Handling Skew                                     /////
  ////     Understanding Map Reduce Shuffle and Sort             /////
  ////         Memory and Spills to Memory, Disk                 ////
  ///////////////////////////////////////////////////////////////////

  // one key or a set of keys have disproportionately large share
  // Two approaches - bifurcate the data and process non skewed and skewed separately and merge
  // Salt the keys to create a new key with more even distribution

  val prvolDFSkewed = prvolDFFmDisk.selectExpr("*",
    "cast(" +
      "case when trdate > '2015-01-01' then trdate else '2014-01-01' end " +
      " as timestamp) as sktsp",
    "case when substring(symbol,0,1) = 'A' then symbol " +
      "else 'CMSYS' end as sksym")

  val cmdf4jSkewed = cmdf4jFmDisk.selectExpr("*",
    "cast(" +
      "case when yr > 2015 then tsp else '2014-01-01' end " +
      " as timestamp) as sktsp",
    "case when substring(symbol,0,1) = 'A' then symbol " +
      "else 'CMSYS' end as sksym")

  // save the two skewed tables so that they can be loaded directly
  // for experimentation
  val prvSkewSaveLocation = "file:///mnt/d/tmp/prvskew"
  val cmSkewSaveLocation = "file:///mnt/d/tmp/cmskew"

  prvolDFSkewed.write.parquet(prvSkewSaveLocation)
  cmdf4jSkewed.write.parquet(cmSkewSaveLocation)
  // val prvolDFSkewed = spark.read.parquet(prvSkewSaveLocation)
  // val cmdf4jSkewed = spark.read.parqueyt(cmSkewSaveLocation)

  // prepare the data
  // make one symbol predominant
  val cmdfSkewedBySymbol = cmdf4jFmDisk.selectExpr("*", "case when substring(symbol, 0, 1) < 'F' then symbol " +
    "else 'SKSYM' end as sksym")

  // create the dimension for symbol and delivery percent
  // same rules as for cash market
  val prvoldfDelPerForSymbolSkew = prvolDFFmDisk.selectExpr("*",
    "case when  substring(symbol, 0, 1) < 'F' then symbol " +
      "else 'SKSYM' end as sksym", "delper").
    groupBy("sksym").
    agg(avg("delper") as "adelper")

  cmdfSkewedBySymbol.selectExpr("floor(rand()*5) as rno").agg(min("rno"), max("rno")).show

  // save the skewed data
  val cmdfSkewedSaveLocation = "file:///mnt/d/tmp/cmdfskew"
  val prvdfSkewedSaveLocation = "file:///mnt/d/tmp/prvskewu"

  cmdfSkewedBySymbol.write.mode("overwrite").save(cmdfSkewedSaveLocation)
  prvoldfDelPerForSymbolSkew.write.mode("overwrite").save(prvdfSkewedSaveLocation)

  // load the skewed data
  val cmdfSkewed = spark.read.parquet(cmdfSkewedSaveLocation)
  val prvdfSkewed = spark.read.parquet(prvdfSkewedSaveLocation)

  // check a regular join
  cmdfSkewed.join(prvdfSkewed, "sksym").count

  // check the salted join
  // we add a column where
  cmdfSkewed.withColumn("rno", floor(rand() * 10)).join(
    prvdfSkewed.selectExpr("*",
      "explode(array(0,1,2,3,4,5,6,7,8,9)) as rno"),
    Array("sksym", "rno")
  ).count

  // check an aggregation procedure using regular keys
  cmdfSkewed.join(prvdfSkewed, "sksym").
    groupBy(cmdfSkewed.col("sksym")).
    agg(sum("qty") as "tqty", avg("adelper") as "adelper").
    orderBy(desc("adelper")).show

  // check an aggregation procedure using salted keys
  cmdfSkewed.withColumn("rno", floor(rand() * 10)).join(
    prvdfSkewed.selectExpr("*",
      "explode(array(0,1,2,3,4,5,6,7,8,9)) as rno"),
    Array("sksym", "rno")
  ).
    groupBy(cmdfSkewed.col("sksym")).
    agg(sum("qty") as "tqty", avg("adelper") as "adelper").
    orderBy(desc("adelper")).show

  val cmdfSkewedLarge = cmdfSkewed.
    union(cmdfSkewed).
    union(cmdfSkewed).
    union(cmdfSkewed).
    union(cmdfSkewed).
    union(cmdfSkewed)

  val cmdfsklargeLocation = "file:///mnt/d/tmp/cmdfsklarge"
  val cmdfSkewedLargeFmDisk = spark.read.parquet(cmdfsklargeLocation)
  cmdfSkewedLarge.
    write.
    mode("overwrite").
    save(cmdfsklargeLocation)

  // set the auto broadcast join threshold to -1 and check the time and spill for the
  val cmdfLargeJoinSaveLocation = "file:///mnt/d/tmp/cmdflargejoinsave"
  cmdfSkewedLargeFmDisk.withColumn("rno", floor(rand() * 10)).join(
    prvdfSkewed.selectExpr("*",
      "explode(array(0,1,2,3,4,5,6,7,8,9,10," +
        "11,12,13,14,15,16,17,18,19)) as rno"),
    Array("sksym", "rno")
  ).write.mode("overwrite").parquet(cmdfLargeJoinSaveLocation)

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
          prTreeSet.add(record)
          if (prTreeSet.size > TOP_PRs)
            prTreeSet.remove(prTreeSet.last)
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
}
