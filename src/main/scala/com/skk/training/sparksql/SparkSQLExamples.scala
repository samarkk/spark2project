package com.skk.training.sparksql
import org.apache.spark.sql.SparkSession

object SparkSQLExamples extends App {
  val spark = SparkSession.builder()
    .appName("SparkSQLExamples")
    .master("local[*]")
    .getOrCreate()

  val sc = spark.sparkContext
  //  println(sc.version + " , " + spark.version)
  sc.setLogLevel("ERROR")

  import spark.implicits._

  // create a dataframe from a rdd
  // see the dataframe api
  // register a table against the dataframe
  // run sql queries against the view, table
  // create a dataset
  // create a case class and see the methods available for dataset
  case class Trans(accNo: String, tranAmount: Double)
  case class AcMaster(accNo: String, firstName: String, lastName: String)
  case class AcBal(accNo: String, balanceAmount: Double)

  val acTransList = Array("SB10001,1000", "SB10002,1200", "SB10003,8000",
    "SB10004,400", "SB10005,300", "SB10006,10000", "SB10007,500",
    "SB10008,56", "SB10009,30", "SB10010,7000",
    "CR10001,7000", "SB10002,-10")
  val acRDD = sc.parallelize(acTransList)

  def toTrans(atr: Seq[String]): Trans = Trans(atr(0), atr(1).toDouble)
  val acTransDF = acRDD.map(_ split ",").map(toTrans(_)).toDF

  println("Case classes lead to an automatic schema generation")
  acTransDF.printSchema()
  println("Use show to look at the first few rows of a dataframe")
  acTransDF.show
  val acDFStraightFromRDD = acRDD.map(_ split ",").map(x => (x(0),
    x(1).toDouble)).toDF("accNo", "tranAmount")
  // under the hood there is a implicit def rddToDataSetHolder in SQLImplicits
  // which creates the DataFrame as a DataSet of GenericRow
  println("The schema can be generated for an RDD specifying the column names")
  acDFStraightFromRDD.printSchema()

  // toDF is a shortuct for this method only
  //  def createDataFrame[A <: Product](rdd: RDD[A])(
  // implicit arg0: scala.reflect.api.JavaUniverse.TypeTag[A]): DataFrame
  // Creates a DataFrame from an RDD of Product (e.g. case classes, tuples).
  val acTransDFFmSQLC = spark.createDataFrame(acRDD.map(_ split ",").map(toTrans(_)))

  println("\nData frame select column, columns")
  // def select(col: String, cols: String*): DataFrame
  acTransDF.select($"accNo").collect.foreach(println)
  println("\n Data fame selection using select expr")
  acTransDF.selectExpr("accNo as account_no", "tranAmount").collect.foreach(println)
  println("\nfitler equivalent to where, operation - filter and where seem to be equivalent")
  acTransDF.filter("tranAmount >= 1000").collect.foreach(println)
  acTransDF.where("tranAmount >= 1000 and accNo like'SB%' ").collect.foreach(println)
  println("Filtering for multiple conditioins using $ columns")
  acTransDF.filter($"tranAmount" >= 1000 and $"accNo".startsWith("SB")).collect.foreach(println)

  import org.apache.spark.sql.functions.{ substring, count, sum }
  println("Grouping using the dataframe api")
  val acGroupedDF = acTransDF.groupBy(substring($"accNo", 0, 2) as "account_type").agg(
    count($"tranAmount") as "no_trans", sum($"tranAmount") as "amount_total")
  acGroupedDF.printSchema
  acGroupedDF.collect().foreach(println)

  // sorting using DataFrames
  val alist = List(("sachin", "tendulkar", 102),
    ("sachin", "tendulkar", 95), ("arjun", "tendulkar", 5), ("arjun", "vendulkar", 5),
    ("arjun", "vendulkar", 10), ("sachin", "vendulkar", 12),
    ("sachin", "vendulkar", 102), ("arjun", "tendulkar", 1))
  case class CPlayer(fname: String, lname: String, cent: Int)
  val cplDF = sc.parallelize(alist).map(x => CPlayer(x._1, x._2, x._3.toInt)).toDF
  println("\nWe can sort using any order of column names by sequencing them and specifying asc / desc")
  cplDF.sort($"lname".desc, $"fname", $"cent" desc).collect.foreach(println)
  // orderBy is an alias for the sort function

  // defining udfs
  //  import spark.udf
  println("\nCreating a udf using spark 2 and registering it to use it in sql queries")
  val tenf: Int => Int = _ * 10
  spark.udf.register("tenudf", tenf)
  spark.udf.register("tenudf1", { (x: Int) => x * 10 })
  cplDF.createOrReplaceTempView("cptbl")
  spark.sql("select *, tenudf(cent) as tenudf_col from cptbl").show
  spark.sql("select *, tenudf1(cent) as tenudf1_col from cptbl").show

  // with spark 2

  import org.apache.spark.sql.functions.udf
  println("\nUsing spark functions udf to wrap a function in udf and " +
    "use it in the dataframe api")
  val tenf2: Int => Int = _ * 10
  val tenudf2 = udf(tenf2)
  val tenudf21 = udf { (x: Int) => x * 10 }
  val tenudf22 = udf[Int, Int](_ * 10)
  cplDF.select(tenudf2('cent)).show
  cplDF.select(tenudf21('cent)).show
  cplDF.select(tenudf22('cent)).show

  val stlist = List("INFY,2017-05-01,2000,2164550",
    "INFY,2017-5-02,1954,2174352",
    "INFY,2017-06-03,2341,2934231",
    "INFY,2017-06-04,1814,1904557",
    "SBIN,2017-05-01,200061,3164550",
    "SBIN,2017-5-02,211954,3174352",
    "SBIN,2017-06-03,222341,3434234",
    "SBIN,2017-06-04,301814,4590455")

  val strdd = sc.parallelize(stlist)
  case class Stock(symbol: String, trdate: String, qty: Int, vlu: Double)
  val stdf = strdd.map(_ split ",").map(x => Stock(x(0), x(1), x(2).toInt, x(3).toDouble)).toDF
  println("Lets take a look at the stocks dataframe")
  stdf.show
  // add year and month columns to the dataframe
  import org.apache.spark.sql.functions.{ year, month, to_date }
  val stdf_wym = stdf.select($"symbol", year(to_date($"trdate")) as "yr",
    month(to_date($"trdate")) as "mnth", $"qty", $"vlu")
  // rdd operations on dataframe
  println("\n manipulating dataframes using rdd operations")
  stdf.map(x => (x.getAs[String](0), x.getAs[String](1), x.getAs[Int](2),
    x.getAs[Double](3))).collect.foreach(println)
  println("We can use the schema information to refer to fields in any order")
  stdf.map(x => (x.getAs[String]("symbol"), x.getAs[Double]("vlu"))).collect.foreach(println)
  // multi dimensional aggregations
  println("\ncube will provide every cobmination of the fields used to create the cube")
  stdf_wym.cube("symbol", "yr", "mnth").agg(sum("qty") as "qty", sum("vlu") as "vlu").show
  println("\nrollup will rollup aggregates beginning from the first field in the rollup columns")
  stdf_wym.rollup("symbol", "yr", "mnth").agg(sum("qty") as "qty", sum("vlu") as "vlu").show

  println("Using sql to carry out multi dimensional aggregations")
  stdf_wym.createTempView("stmdtbl")

  // grouping sets available in spark 2
  println("\nspark 2 grouping sets")
  spark.sql("""select symbol,yr,mnth,sum(qty) as qty, sum(vlu) as vlu
    from stmdtbl
    group by symbol, yr, mnth
    grouping sets ((symbol,yr),(yr,mnth))""").show

  println("\nSpark 2 cube sql")
  spark.sql("""select symbol,yr,mnth,sum(qty) as qty, sum(vlu) as vlu
    from stmdtbl
    group by symbol, yr, mnth
    with cube""").show

}
