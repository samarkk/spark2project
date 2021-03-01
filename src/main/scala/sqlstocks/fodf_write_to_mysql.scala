package sqlstocks

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

// in focm_analysis - save the csv files as a the fotbl - default format parquet used will reduce the size significantly
val fodf = spark.read.table("fotbl")
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
    calendar.get(mname.toUpperCase) match {
      case None => dt
      case Some((mn)) => dt.substring(dt.length - 4, dt.length) + "-" + mn + "-" + dt.substring(0, 2)
    }
  }

  // create a udf from he function
  val udf_mname_to_no = udf(mnameToNo)
  // to use udf in sql, we have to register it
  spark.udf.register("udf_mname_to_no", mnameToNo)
  fodf.createOrReplaceTempView("fut_data")
  val futPLDF =
  spark.sql(
    """
select to_timestamp(udf_mname_to_no(timestamp), "yyyy-mm-dd") as tsp,
f.*, b_s_pl_status - close * open_int as fin_status from
(
select timestamp, symbol,expiry_dt,chg_in_oi,close,open_int,
sum(chg_in_oi) over (partition by symbol,expiry_dt order by to_timestamp(udf_mname_to_no(timestamp), "yyyy-mm-dd")) as cum_chg_oi,
sum(chg_in_oi*close) over ( partition by symbol,expiry_dt order by to_timestamp(udf_mname_to_no(timestamp), "yyyy-mm-dd"))
as b_s_pl_status
from fut_data
where instrument like 'FUT%'
) f
where lower(f.expiry_dt) = lower(f.timestamp)
""")

import java.util.Properties
val props = new Properties
props.put("driver", "com.mysql.cj.jdbc.Driver")
props.put("user", "root")
props.put("password", "abcd")

futPLDF.write.mode("overwrite").jdbc(
    "jdbc:mysql://localhost:3306/testdb",
    "futpltbl", props
)

/*
sql queries
select case when fin_status < 0 then 'sellers' else 'buyers' end as whowon from futpltbl limit 5

select symbol, whowon, count(*) as nowins from
(
select symbol, case when fin_status < 0 then 'sellers' else 'buyers' end as whowon
from futpltbl
) f
group by symbol, whowon
order by symbol, whowon
*/

val prvolDFLocation = "file:///mnt/d/tmp/prvoldf"
val prvolDFFmDisk = spark.read.parquet(prvolDFLocation)
  
prvolDFFmDisk.filter("year(trdate) between 2018 and 2019").groupBy("symbol").agg(avg("delper") as "delper").show

prvoldf.write.saveAsTable("prvol1819tbl")
prvoldf.write.jdbc("jdbc:mysql://localhost:3306/testdb", "prvoltbl", props);

/*
sql join query
select f.*, p.delper from futpltbl f inner join prvoltbl p on f.symbol = p.symbol;
*/
