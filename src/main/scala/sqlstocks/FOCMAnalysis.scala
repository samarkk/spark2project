package sqlstocks

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

object FOCMAnalysis extends App {
  // create the spark session
  val spark = SparkSession.builder()
    .appName("FOAnalytics")
    .master("local[*]").getOrCreate
  // get the spark context and set level to error
  val sc = spark.sparkContext
  sc.setLogLevel("ERROR")

  import spark.implicits._

  // set the location for the cash market data
  val cmDataLocation = "file:///mnt/d/findataf/201819/cm/"

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

  // number of partitions of cmdf
  //  spark.sql.openCostInBytes = 4 MB
  // (no of files x  4 MB + data size) / block size
  println(s"Number of partitions ${cmdf.rdd.getNumPartitions}")

  // specify the location for the futures and options data
  val foDataLocation = "file:///mnt/d/findataf/201819/fo/"

  // read the data, infer schema and header is there in the files
  val fo_df = spark.
    read.
    option("inferSchema", value = true).
    option("header", value = true).
    csv(foDataLocation)

  // drop the extra column created due to the trailing comma
  val fodf = fo_df.drop("_c15")

  // a function to replace month names with numbers
  // create a function literal to replace monthnames with numbers
  // we want to transform 31-OCT-2019 to 31-10-2019
  // so we will create a map from JAN, FEB etc to 01, 02 etc
  // and use string replace to replace JAN with 01, FEB with 02 etc and son on
  val mnameToNo = (dt: String) => {
    val mname = dt.substring(3, 3 + 3)
    val calendar = Map[String, String]("JAN" -> "01", "FEB" -> "02", "MAR" -> "03", "APR" -> "04",
      "MAY" -> "05", "JUN" -> "06", "JUL" -> "07", "AUG" -> "08", "SEP" -> "09", "OCT" -> "10",
      "NOV" -> "11", "DEC" -> "12")
    dt.replace(mname, calendar(mname.toUpperCase))
  }

  // create a udf from he function
  val udf_mname_to_no = udf(mnameToNo)

  println("verifying that we our udf works and we can plug in to_timestamp")
  fodf.select(to_timestamp(udf_mname_to_no($"TIMESTAMP"), "dd-MM-yyyy")).show

  fodf.createOrReplaceTempView("fut_data")

  println("taking a look at settlement date data")
  spark.sql("select * from fut_data where lower(expiry_dt) = lower(timestamp)limit 5").show(3)

  println("settlement data look using df api")
  fodf.where("lower(expiry_dt) = lower(timestamp)").limit(5).show

  println("settlement data look using df api using column names")
  fodf.where(lower($"EXPIRY_DT") === lower($"TIMESTAMP")).limit(5).show

  // F&O Futures Net PL estimation
  // Lets say we have the following trades for 3/4 days
  // Day  Symbol Long/Short Qty Price  TotalOutlay  OI    CHG_OI
  //  1    X         Buy    100   20      2000      100    100
  //  2    X         Buy     50   30      3500      150     50
  //  3    X         Sell   100   10      2500       50   -100
  // Lets say close price on the third day was 12. Then further 12 x 50 needs to be deducted to find out the net settlement amount.
  // So we would have 2500 - 600 - a profit of 1900 made by the sellers

  // Lets check a profitable scenario for the buyers
  // Day  Symbol Long/Short Qty Price  TotalOutlay  OI    CHG_OI
  //  1     X    Buy         100   20      2000     100   100
  //  2     X    Buy          50   30      3500     150     50
  //  3     X    Sell        100   40      -500      50   -100
  // Lets say close price on the third day was 42. Then further 42 x 50 needs to be deducted to find out the net settlement amount.
  // So we would have -500-2100 - a profit of 2600 made by the buyers

  // Here we take a look at cumulative, rolling profit
  // status for buyers and sellers
  // We can consider a positive value to indicate that buyers made a profit
  // as the sale price was more than the buy one
  // and vice versa that the short side was in profit if we have a negative figure

  // to use udf in sql, we have to register it
  spark.udf.register("udf_mname_to_no", mnameToNo)

  println("Using sql to show overall profit loss status for f&o market")
  spark.sql(
    """
select timestamp, symbol,expiry_dt,chg_in_oi,close,open_int,
sum(chg_in_oi)
over (partition by symbol,expiry_dt order by to_timestamp(udf_mname_to_no(timestamp), "dd-MM-yyyy")) as cum_chg_oi,
sum(chg_in_oi*close)
over ( partition by symbol,expiry_dt order by to_timestamp(udf_mname_to_no(timestamp), "dd-MM-yyyy"))
as b_s_pl_status
from fut_data
where instrument like 'FUT%' and symbol = 'INFY' and expiry_dt = '31-May-2018'
order by to_timestamp(udf_mname_to_no(timestamp), "dd-MM-yyyy")
""").show(100, truncate = false)
  // prior settlement dates 28-Mar-2019, 26-Apr-2019

  // create a view with the proper timestamp column - tsp -timestamp proper
  val fodfvw = fodf.
    withColumn("tsp",
      to_timestamp(udf_mname_to_no($"TIMESTAMP"), "dd-MM-yyyy")
    )

  // window functions using dataframe api
  // we need to create the window spec
  val partitionWindow = Window.
    partitionBy($"symbol", $"expiry_dt").
    orderBy("tsp")
  // then define the aggregation functions we are interested in over it
  val sumChgoi = sum($"CHG_IN_OI").over(partitionWindow)
  val plStatus = sum($"CHG_IN_OI" * $"close").over(partitionWindow)

  // and plug in the regular dataframe api

  println("window functions executed using df api")
  fodfvw.
    select($"tsp", $"symbol",
      $"expiry_dt", $"chg_in_oi", $"close", $"open_int",
      sumChgoi.as("oitrack"), plStatus.as("plstatus")).
    filter("symbol = 'INFY' " +
      "and instrument like 'FUT%' " +
      "and expiry_dt = '31-May-2018'").
    orderBy($"tsp").
    show(100, truncate = false)

  // here we check the status on the expiry dates for the pl situation
  println("status on the expiry dates for the pl situation")
  spark.sql(
    """
select f.* from
(
select timestamp, symbol,expiry_dt,chg_in_oi,close,open_int,
sum(chg_in_oi) over (partition by symbol,expiry_dt order by to_timestamp(udf_mname_to_no(timestamp), "dd-MM-yyyy")) as cum_chg_oi,
sum(chg_in_oi*close) over ( partition by symbol,expiry_dt order by to_timestamp(udf_mname_to_no(timestamp), "dd-MM-yyyy"))
as b_s_pl_status
from fut_data
where instrument like 'FUT%'
) f
where lower(f.expiry_dt) = lower(f.timestamp) and symbol = 'INFY'
order by to_timestamp(udf_mname_to_no(f.timestamp), "dd-MM-yyyy")
""").show(50, truncate = false)
  // add on the line below insted of .show to see for each expiry period
  // .selectExpr("*", "b_s_pl_status - close * open_int as fin_status").show(50, false)

  // create a proper timestamp column for the cash market data
  val cmdfvw = cmdf.withColumn(
    "tsp",
    to_timestamp(udf_mname_to_no($"TIMESTAMP"), "dd-MM-yyyy"))
  cmdfvw.createOrReplaceTempView("cmdata")

  println("verifying the extra column we added as a proper timestamp")
  cmdfvw.limit(5).show

  // use sql to carry out window functions moving average using rows preceding
  println("using sql to carry out window functions moving average using rows preceding")
  spark.sql(
    """
select symbol, timestamp,close,
avg(close) over(partition by symbol order by tsp rows 5 preceding )as mv5avg,
avg(close) over(partition by symbol order by tsp rows 20 preceding )as mv20avg
from cmdata
where symbol = 'INFY'
order by tsp
""").show()

  // using dataframe api to calculate moving average
  println("using dataframe api to calculate moving average")
  val mvngAvgSpec = avg($"close").
    over(Window.partitionBy($"symbol").
      orderBy($"tsp").rowsBetween(-3, 0))

  cmdfvw.select($"symbol", $"timestamp", $"tsp", $"close",
    mvngAvgSpec.as("mvng_avg_5")).filter("symbol = 'INFY'").
    orderBy("tsp").show

  // exercise - add on 20 days moving average

  // using row number

  println("using row number with sql")
  spark.sql(
    """
select symbol, timestamp, close,
row_number() over(partition by symbol order by tsp) as rno
from cmdata
where symbol in ('INFY', 'TCS')""").show

  // dafaframe api using row number
  println("dafaframe api showing row number")
  val rnospec = row_number()
    .over(Window.partitionBy($"symbol")
      .orderBy($"tsp"))

  cmdfvw.select($"symbol", $"timestamp", $"close",
    rnospec.as("rno"))
    .filter("symbol in ('INFY','TCS')")
    .show

  // The ranking function enables us to rank data by particular attribute

  // Here we use sql api to rank by quantity traded for each symbol anc check results for one
  spark.sql(
    """
select symbol, timestamp, TOTTRDQTY,
rank() over(partition by symbol order by TOTTRDQTY desc)as cumqty
from cmdata
where symbol = 'INFY'
order by tsp
""").show()

  // ranking with the dataframe api
  val rankspec = rank().
    over(Window.partitionBy($"symbol").
      orderBy($"TOTTRDQTY" desc))

  cmdfvw.select($"symbol", $"timestamp", $"TOTTRDQTY", rankspec.as("rank")).
    filter("symbol in ('INFY', 'TCS')").show

  // cumulative distribution can tell us the total percentage of values covered till each value
  // Here we use sql to explore the cumulative distribution for closing prices
  // and we find that 10% of the values are till 673 and 50% of the values are till 755
  spark.sql(
    """
select symbol, timestamp, close,
cume_dist() over(partition by symbol order by close) as cumdist
from cmdata
where symbol = 'INFY'
order by cumdist
""").show(250, truncate = false)

  // cume_dist using dataframe api
  cmdfvw.select('symbol, 'timestamp, 'close,
    cume_dist.over(Window.partitionBy("symbol").orderBy("close")).as("cumdist")).
    filter("symbol = 'INFY'").orderBy("cumdist").show(250, truncate = false)

  // percent_rank is similar to rank except it will rank from 0 to 1.0
  // percent_rank using sql
  spark.sql(
    """
select symbol, timestamp, TOTTRDQTY,
percent_rank() over(partition by symbol order by TOTTRDQTY) as pctrank
from cmdata
where symbol = 'INFY'
order by pctrank desc
""").show(250)

  cmdfvw.select('symbol, 'timestamp, 'tottrdqty,
    percent_rank.over(Window.partitionBy("symbol").orderBy("tottrdqty")).as("pctrank")).
    filter("symbol = 'INFY'").orderBy(desc("pctrank")).show(250)

  // dense rank will ensure continuous ranks telling us the total ranks in a dataset
  // against rank which will have the largest rank equal to number of elements in the datataset
  // we will use this exaample to  undestand
  // we create a dataframe to check difference between rank and dense_rank

  val drillf = sc.parallelize(List(("a", 1), ("a", 2), ("a", 2), ("a", 3), ("a", 4))).toDF("char", "no")

  // the largest rank with rank is five with commaon rank for two common tuples. rank 3 will be missing
  drillf.select($"char", $"no", rank().over(Window.partitionBy("char").orderBy($"no")).as("rank")).show

  // with dense_rank it is four. there are no missing ranks
  drillf.select($"char", $"no", dense_rank().over(Window.partitionBy("char").orderBy($"no")).as("denserank")).show

  // We now see dense_rank using sql to find the dense ranks for a stock according to its closing price
  // we find that we have the highest rank of 233 for INFY whereas earlier this rank was 247
  // dense_rank using sql
  spark.sql(
    """
select symbol, timestamp, close,
dense_rank() over(partition by symbol order by close) as denserank
from cmdata
where symbol = 'INFY'
order by denserank desc
""").show()

  cmdfvw.select('symbol, 'timestamp, 'close,
    dense_rank.over(Window.partitionBy("symbol").orderBy("close")).as("denserank")).
    filter("symbol = 'INFY'").orderBy(desc("denserank")).show

  // ntile is similar to percentile except it allows us to divide into customized number of bins instead of the default 100 used with percentiles
  // here we use ntile 20 and min and max to find the buckets for trading quantity for a particular stock across a year of trading

  // ntile using sql
  spark.sql(
    """
select cntile, min(TOTTRDQTY) as minqty, max(TOTTRDQTY) as maxqty from (
select symbol, timestamp, TOTTRDQTY,
ntile(20) over(partition by symbol order by TOTTRDQTY) as cntile
from cmdata
where symbol = 'INFY'
) ntv
group by cntile
order by cntile
""").show()

  // ntile using dataframe api
  val ntilespec = ntile(20).
    over(Window.partitionBy($"symbol").
      orderBy($"TOTTRDQTY"))

  cmdfvw.select('symbol, 'timestamp, 'TOTTRDQTY, ntilespec.as("cntile")).
    filter("symbol = 'INFY'").
    groupBy($"cntile").
    agg(min($"TOTTRDQTY") as "minqty", max($"TOTTRDQTY") as "maxqty").
    orderBy($"cntile").show


  // lead and lag allow us to look back, look ahead
  // here we use them to look at the stock price oscillations across five days
  // two prior and two next2

  // lead and lag using sql
  spark.sql(
    """
SELECT F.* FROM (
SELECT SYMBOL, TIMESTAMP,
LAG(CLOSE, 2) OVER(PARTITION BY SYMBOL ORDER BY TSP) AS PREV2CLOSE,
LAG(CLOSE, 1) OVER(PARTITION BY SYMBOL ORDER BY TSP) AS PREVCLOSE,
CLOSE,
LEAD(CLOSE, 1) OVER(PARTITION BY SYMBOL ORDER BY TSP) AS NEXTCLOSE,
LEAD(CLOSE, 2) OVER(PARTITION BY SYMBOL ORDER BY TSP) AS NEXT2CLOSE
FROM CMDATA
) F
WHERE TIMESTAMP = '19-NOV-2019' AND SYMBOL IN ('INFY', 'SBIN', 'TCS', 'ITC', 'HDFCBANK')
ORDER BY SYMBOL
""").show()

  // lead and lag using the dataframe api
  cmdfvw.select($"symbol", $"timestamp",
    lag($"close", 2).over(Window.partitionBy($"symbol").orderBy($"tsp")).as("prev2close"),
    lag($"close", 1).over(Window.partitionBy($"symbol").orderBy($"tsp")).as("prevclose"),
    $"close",
    lead($"close", 1).over(Window.partitionBy($"symbol").orderBy($"tsp")).as("nextclose"),
    lead($"close", 2).over(Window.partitionBy($"symbol").orderBy($"tsp")).as("next2close")).
    filter("TIMESTAMP = '19-NOV-2019' AND SYMBOL IN ('INFY', 'SBIN', 'TCS', 'ITC', 'HDFCBANK')").
    orderBy("symbol").show

  // creating a view that we will use to cube by year, month, day
  val cmdfymdvw = cmdfvw
    .withColumn("yr", year($"tsp"))
    .withColumn("mnth", month($"tsp"))
    .withColumn("dy", dayofmonth($"tsp"))

  cmdfymdvw.createOrReplaceTempView("cb_tbl")

  // grouping sets using sql
  println("grouping sets using sql")
  spark.sql(
    """
select symbol, yr, mnth, dy , avg(close) as avgcls, avg(tottrdqty) as avgqty, avg(tottrdval) as avgval from cb_tbl
where symbol in ('INFY', 'TCS')
group by symbol, yr, mnth, dy
grouping sets((yr), (yr, mnth))
""").show

  // cube using sql
  println("cube using sql")
  spark.sql(
    """
select symbol,yr,mnth,dy,
avg(close) as avgcls,
avg(tottrdqty) as avgqty,
avg(tottrdval) as avgval
from cb_tbl
group by symbol,yr,mnth,dy with cube
order by yr ,mnth""").show()

  // cube using dafaframe api
  println("cube using dafaframe api")
  cmdfymdvw.cube($"symbol", $"yr", $"mnth", $"dy")
    .agg(
      avg("close").as("avgcls"),
      avg("tottrdqty").as("avgqty"),
      avg($"tottrdval").as("tottrdval"))
    .show

  // cube - see the overall aggregates

  println("cube - see the overall aggregates")
  cmdfymdvw.cube($"symbol", $"yr", $"mnth", $"dy").
    agg(
      avg("close").as("avgcls"),
      avg("tottrdqty").as("avgqty"),
      avg($"tottrdval").as("tottrdval"))
    .filter(isnull($"symbol")).show

  // rolling using sql

  println("rolling using sql")
  spark.sql(
    """
select symbol,yr,mnth,dy,
avg(close) as avgcls, avg(tottrdqty) as avgqty,
avg(tottrdval) as avgval from cb_tbl
where symbol = 'INFY' and mnth = 1
group by symbol,yr,mnth,dy with rollup
order by yr ,mnth, dy""")
    .show()

  // rollup dataframe - just replace cube with rollup
  // difference between rollup and cube
  // rollup(yr, mnth, dy) - (), (yr, mnth, dy), (yr, mnth), (yr)
  // cube(yr, mnth, dy) - (), (dy), (mnth), (yr), (yr, mnth), (yr, dy_), (mnth, dy), (yr, mnth, dy)
  // rollup maintains hierarchy cube does not

}


