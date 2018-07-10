package sqlstocks

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

object FOCMAnalysis extends App {
  val spark = SparkSession.builder()
    .appName("FOAnalytics")
    .master("local[*]").getOrCreate
  val sc = spark.sparkContext
  sc.setLogLevel("ERROR")
  import spark.implicits._

  val cm_df = spark.read
    .option("inferSchema", true)
    .option("header", true)
    .csv("hdfs://localhost:8020/user/cloudera/findata/cm")

  val cmdf = cm_df.drop("_c13")
  println("The cash market data frame schema")
  cmdf.printSchema

  val fo_df = spark.read
    .option("inferSchema", true)
    .option("header", true)
    .csv("hdfs://localhost:8020/user/cloudera/findata/fo")

  val fodf = fo_df.drop("_c15")

  // a function to replace month names with numbers
  val mnameToNo = (dt: String) => {
    val mname = dt.substring(3, 3 + 3)
    val calendar = Map[String, String]("JAN" -> "01", "FEB" -> "02", "MAR" -> "03", "APR" -> "04",
      "MAY" -> "05", "JUN" -> "06", "JUL" -> "07", "AUG" -> "08", "SEP" -> "09", "OCT" -> "10",
      "NOV" -> "11", "DEC" -> "12")
    dt.replace(mname, calendar(mname))
  }

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
  spark.sql("""
select timestamp, symbol,expiry_dt,chg_in_oi,close,open_int,
sum(chg_in_oi)
over (partition by symbol,expiry_dt order by to_timestamp(udf_mname_to_no(timestamp), "dd-MM-yyyy")) as cum_chg_oi,
sum(chg_in_oi*close)
over ( partition by symbol,expiry_dt order by to_timestamp(udf_mname_to_no(timestamp), "dd-MM-yyyy"))
as b_s_pl_status
from fut_data
where instrument like 'FUT%' and symbol = 'INFY'
order by to_timestamp(udf_mname_to_no(timestamp), "dd-MM-yyyy")
""").show(50, false)

  // create a view with the proper timestamp column - tsp -timestamp proper
  val fodfvw = fodf.withColumn("tsp", to_timestamp(udf_mname_to_no($"TIMESTAMP"), "dd-MM-yyyy"))

  // window functions using dataframe api
  // we need to create the window spec
  val partitionWindow = Window.partitionBy($"symbol", $"expiry_dt").orderBy("tsp")
  // then define the aggregation functions we are interested in over it
  val sumChgoi = sum($"CHG_IN_OI").over(partitionWindow)
  val plStatus = sum($"CHG_IN_OI" * $"close").over(partitionWindow)

  // and plug in the regular dataframe api
  println("window functions executed using df api")
  fodfvw.select($"symbol", $"tsp", $"timestamp",
    $"expiry_dt", $"chg_in_oi", $"open_int", $"close",
    sumChgoi.as("oitrack"), plStatus.as("plstatus"))
    .filter("symbol = 'INFY' and instrument like 'FUT%'")
    .orderBy($"tsp")
    .show(20, false)

  // here we check the status on the expiry dates for the pl situation
  println("status on the expiry dates for the pl situation")
  spark.sql("""
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
""").show(50, false)

  // create a proper timestamp column for the cash market data
  val cmdfvw = cmdf.withColumn(
    "tsp",
    to_timestamp(udf_mname_to_no($"TIMESTAMP"), "dd-MM-yyyy"))
  cmdfvw.createOrReplaceTempView("cmdata")

  println("verifying the extra column we added as a proper timestamp")
  cmdfvw.limit(5).show

  // use sql to carry out window functions moving average using rows preceding
  println("using sql to carry out window functions moving average using rows preceding")
  spark.sql("""
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
    mvngAvgSpec.as("mvng_avg_5")).filter("symbol = 'INFY'")
    .orderBy("tsp").show

  // exercise - add on 20 days moving average

  // using row number

  println("using row number with sql")
  spark.sql("""
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

  // creating a view that we will use to cube by year, month, day
  val cmdfymdvw = cmdfvw
    .withColumn("yr", year($"tsp"))
    .withColumn("mnth", month($"tsp"))
    .withColumn("dy", dayofmonth($"tsp"))

  cmdfymdvw.createOrReplaceTempView("cb_tbl")

  // grouping sets using sql
  println("grouping sets using sql")
  spark.sql("""
select symbol, yr, mnth, dy , avg(close) as avgcls, avg(tottrdqty) as avgqty, avg(tottrdval) as avgval from cb_tbl
where symbol in ('INFY', 'TCS')
group by symbol, yr, mnth, dy
grouping sets((yr), (yr, mnth))
""").show

  // cube using sql
  println("cube using sql")
  spark.sql("""
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
  spark.sql("""
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
