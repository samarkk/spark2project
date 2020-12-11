package sparksql

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Dataset
import datagen.Generator._
import org.apache.spark.sql.functions._

object DPPDemo extends App {

  val spark = SparkSession.builder()
    .appName("DPPExample")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  val sc = spark.sparkContext

  // generate the data
  val mobiles = generateMobileModels(200, 20)
  val mobileSales = generateMobileSales(1000000, 20)

  // create DataFrames
  val mobilesDF = spark.createDataFrame(sc.parallelize(mobiles))
  val salesDF = spark.createDataFrame(sc.parallelize(mobileSales))

  salesDF.show

  // check join
  val salesJoined = salesDF.join(mobilesDF, Seq("mobileid", "modelid"))
  salesJoined.show
  // with a 2 million data size this guy chokes
  salesJoined.groupBy("cores", "memory").agg(sum('units).as("sales_units")).show

  // force a broadcast join and check
  val salesBroadcastJoined = salesDF.join(broadcast(mobilesDF), Seq("mobileid", "modelid"))
  // with a 2 millin set this guy works
  salesBroadcastJoined.show
  // this guy stiill chokes
  salesBroadcastJoined.groupBy("cores", "memory").agg(sum('units).as("sales_units")).show

  // save to parquet
  val salesFileLocation = "file:///D:/tmp/salesdf"
  val mobileFilesLocation = "file:///D:/tmp/mobiledf"
  salesDF.write.mode("overwrite").partitionBy("mobileid").parquet(salesFileLocation)
  mobilesDF.write.mode("pverwrote").parquet(mobileFilesLocation)

  // load from parquet
  val sdff = spark.read.parquet(salesFileLocation)
  val mdff = spark.read.parquet(mobileFilesLocation)

  sdff.join(mdff, "mobileid").explain
  // cofirm dpp
  // run explain and then run show
  sdff.join(mdff, "mobileid").groupBy("state", "city").agg(sum('units).as("sales_units")).explain
  sdff.join(mdff, "mobileid").groupBy("state", "city").agg(sum('units).as("sales_units")).show

  // dpp in action
  // filter on cores which is not a part of the join condition sees  SubqueryBroadcast dynamicpruning#242,
  sdff.join(mdff, "mobileid").where("cores = 4").groupBy("state", "city").agg(sum('units).as("sales_units")).explain
  sdff.join(mdff, "mobileid").where("cores = 4").groupBy("state", "city").agg(sum('units).as("sales_units")).show

  // spark performs column pruning out of the box
  // here we are selecting four columns after the join but spark does that already in its planning
  // it prunes the columns and uses only what we will take
  sdff.join(mdff, "mobileid").select("mobileid", "state", "city", "units").explain

  // Bucketing
  // write salesdf to a bucketed table
  salesDF.write.bucketBy(4, "state", "city").saveAsTable("bcktbl")
  // read it back
  val bcktDF = spark.read.table("bcktbl")
  // check the query plans for salesDF and bkctDF
  // bcktDF the exchange is nto there - no shuffle
  bcktDF.groupBy("state", "city").agg(sum($"units").as("sunits")).explain
  bcktDF.groupBy("state", "city").agg(sum($"units").as("sunits")).show
  // exchnage is there for salesDF
  salesDF.groupBy("state", "city").agg(sum($"units").as("sunits")).explain
  // if there is selectivity applicaiton of filters will lead to bucket pruning also

  // salting
  val mobileSalesSkewed = generateMobileSales(100000, 20, false)
  val mobileSkewedDF = spark.createDataFrame(sc.parallelize(mobileSalesSkewed))
  mobilesDF

  mobileSkewedDF.join(mobilesDF, Seq("mobileid", "modelid")).groupBy("state", "city").agg(sum("units").as("sunits"), avg("units").as("aunits")).explain
  mobileSkewedDF.join(mobilesDF, Seq("mobileid", "modelid")).groupBy("state", "city").agg(sum("units").as("sunits"),
    avg("units").as("aunits")).count

  val mobileSaltedDF = mobilesDF.withColumn("salt", explode(lit((0 to 99).toArray)))
  val mobileSalesSaltedDF = mobileSkewedDF.withColumn("salt", monotonically_increasing_id() % 100)

  mobileSalesSaltedDF.join(mobileSaltedDF, Seq("mobileid", "modelid", "salt")).groupBy("state", "city").agg(sum("units").as("sunits"), avg("units").as("aunits")).count

}

/*
create a dataframe from a collection
join a large one with a small one
broadcast hash join will not be there

use broadcast function in the join statement
i.e largedf.join(broadcast(smalldf), columns)

add repartition to both dataframes
the number of partitions of resulting dataframe is as per the argument given in
repartition or the default spark.sql.shuffle.partitions

reparition both dataframes by the columns you want to join on

read from parquet and broadcast condition will be picked up automatically

filter pushdown transmits to parquet files

check out grouping
check out bucketing section in the code
 */