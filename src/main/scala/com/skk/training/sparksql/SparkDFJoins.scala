package com.skk.training.sparksql

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Dataset

object SparkDFJoins extends App {

  case class Trans(accNo: String, tranAmount: Double)

  val spark = SparkSession.builder()
    .appName("SparkSQLDS")
    .master("local[*]")
    .getOrCreate()

  val sc = spark.sparkContext
  //  println(sc.version + " , " + spark.version)
  sc.setLogLevel("ERROR")
  import spark.implicits._
  // create two DFs for illustrating joins
  // we have the test df where we have stats of sachin tendulkar
  // and virat kohli for some years
  // sachin has id 1 and virat id 1
  // overall we have 8 records - 4 for sachin, 4 for virat

  val testDF = sc.parallelize(
    List(
      (1, "sachin", 2000, 6, 10, 575, 2),
      (1, "sachin", 2001, 10, 18, 1003, 3),
      (1, "sachin", 2002, 16, 26, 1392, 4),
      (1, "sachin", 2010, 14, 23, 1562, 7),
      (2, "virat", 2011, 5, 9, 202, 0),
      (2, "virat", 2012, 9, 16, 689, 3),
      (2, "virat", 2016, 12, 18, 1215, 4),
      (2, "virat", 2017, 10, 16, 1059, 5))).
    toDF("id", "fname", "year", "matches", "innings", "runs", "centuries")

  // we have the odi df where we have 1 and 2 ids and some years same as
  // for test matches and some years different

  val odiDF = sc.parallelize(
    List(
      (1, 2000, "sachin", 34, 34, 1328, 3),
      (1, 2001, "sachin", 17, 16, 904, 4),
      (1, 2010, "sachin", 2, 2, 1, 204),
      (1, 2011, "sachin", 11, 11, 513, 2),
      (2, 2008, "virat", 5, 5, 159, 0),
      (2, 2009, "virat", 10, 8, 325, 1),
      (2, 2016, "virat", 10, 10, 739, 3),
      (2, 2017, "virat", 26, 26, 1460, 6))).
    toDF("id", "year", "fname", "matches", "innings", "runs", "centuries")

  //  cross join
  // need to have this setting for cross joins to work with spark2 by default
  spark.conf.set("spark.sql.crossJoin.enabled", "true")
  testDF.join(odiDF).show
  println("Cross join will join each of the 8 rows of one df with the " +
    " 8 of the other giving us a total of 64 rows: " +
    testDF.join(odiDF).count)

  //  cross join and column renaming to create unique columns
  println("\ncross join and column renaming to create unique columns")
  testDF.join(odiDF).toDF("id", "fname", "year", "tmatches",
    "tinnings", "truns", "tcenturies", "sid", "syear",
    "sfname", "omatches", "oinnings", "oruns", "ocenturies").show

  //  implicit inner join
  println("join specifying a column name is implicitly an inner join")
  // we will have 32 rows in this join
  // 4 of sachin with id 1 in testdf joining with 4 of sachin with id 1 in odidf
  // and likewise for virat
  testDF.join(odiDF, "id").show
  println("Row produced in inner join: " + testDF.join(odiDF, "id").count)
  // implicit inner join using two columns
  println("\nWe will have only five rows as we have 3 ids and years " +
    " common for sachin and two for virat across the two dfs")
  testDF.join(odiDF, Array("id", "year")).show

  // explicit inner join
  println("\bUsing join expression and explicit inner join")
  testDF.join(odiDF, testDF("id") === odiDF("id"), "inner").show
  // explicit inner join using multiple columns
  println("\nUsing Sequence of columns to carry out an explicit inner join")
  testDF.join(odiDF, Array("id", "year"), "inner").show
  println("\nUsing multiple columns and join expressions to carry out an inner join")
  testDF.join(odiDF, testDF("id") === odiDF("id")
    && testDF("year") === odiDF("year"), "inner").show

  // outer joins
  println("\nLeft outer join")
  val loj = testDF.join(odiDF, testDF("id") === odiDF("id"), "left_outer")
  loj.show()
  println("No of rows produced by left outer join " + loj.count)

  println("\nLeft outer join using multiple columns")
  val lojmc = testDF.join(odiDF, testDF("id") === odiDF("id") &&
    testDF("year") === odiDF("year"), "left_outer")
  lojmc.show()
  println("No of rows produced by left outer join using mulitple columns " + lojmc.count)

  // see the null values for some particular column
  testDF.join(odiDF, Array("id", "year"), "leftOuter").
    toDF("id", "year", "fname", "tmatches",
      "tinnings", "truns", "tcenturies", "sfname",
      "omatches", "oinnings", "oruns", "ocenturies").
      where($"omatches".isNull).show

  // drop all null values
  println("The null values produced during the left outer join")
  testDF.join(odiDF, Array("id", "year"), "leftOuter")
    .toDF("id", "year", "fname", "tmatches",
      "tinnings", "truns", "tcenturies", "sfname",
      "omatches", "oinnings", "oruns", "ocenturies").na.drop().show

  // to filter out null values for a column
  println("\nFiltering out the null values from the left outer join")
  testDF.join(odiDF, Array("id", "year"), "leftOuter")
    .toDF("id", "year", "fname", "tmatches", "tinnings",
      "truns", "tcenturies", "sfname", "omatches",
      "oinnings", "oruns", "ocenturies").where(!$"omatches".isNull).show

  // isnull then 0 else value for a column with null values
  println("Priting a 0 for a null otherwise value for the null value entries")
  import org.apache.spark.sql.functions._
  testDF.join(odiDF, Array("id", "year"), "leftOuter")
    .toDF("id", "year", "fname", "tmatches", "tinnings", "truns",
      "tcenturies", "sfname", "omatches", "oinnings", "oruns", "ocenturies")
    .select($"id", $"year", $"fname", $"tmatches", $"tinnings",
      $"truns", $"tcenturies", when(isnull($"omatches"), 0)
        .otherwise($"omatches").as("omtnull")).show

  // adding a computed column
  println("\nAdding a computed column")
  testDF.join(odiDF, Array("id", "year"), "leftOuter")
    .toDF("id", "year", "fname", "tmatches", "tinnings", "truns",
      "tcenturies", "sfname", "omatches", "oinnings", "oruns", "ocenturies")
    .select($"id", $"year", $"fname", $"tmatches", $"tinnings", $"truns",
      $"tcenturies", when(isnull($"omatches"), 0)
        .otherwise($"omatches").as("omtnull"))
    .withColumn("totmatches", $"tmatches" + $"omtnull").show

  println("\nRight outer join")
  testDF.join(odiDF, testDF("id") === odiDF("id"), "right_outer").show
  println("\nFull outer join")
  val foj = testDF.join(odiDF, testDF("id") === odiDF("id") && testDF("year") === odiDF("year"), "full_outer")
  foj.show()
  // we will have 11 rows in the full outer join for each of the distinct
  // 11 id and year values across the two dataframes
  println("Rows produced in the full outer join using the id and year columns: " + foj.count)

  // leftsemi join
  println("\nLeft semi join - show only those values from the left table " +
    " where the join keys exist in the right table")
  testDF.join(odiDF, testDF("id") === odiDF("id"), "left_semi").show

  // self join
  println("\nSelf join")
  testDF.as("a").join(testDF.as("b")).where($"a.id" === $"b.id").show

  println("\nFiltering on the self join")
  testDF.as("a").join(testDF.as("b"))
    .where($"a.id" === $"b.id" && $"a.year" === $"b.year" && $"a.innings" > 10)
    .show

  // join, grouping and aggregation
  println("\nJoin, grouping and aggregation")
  testDF.groupBy($"year").agg(sum($"runs").as("totruns"), sum($"centuries").as("totcents"), sumDistinct($"centuries").as("distcent"))show

  val testRDD = sc.parallelize(
    List(
      (1, "sachin", 2000, 6, 10, 575, 2),
      (1, "sachin", 2001, 10, 18, 1003, 3),
      (1, "sachin", 2002, 16, 26, 1392, 4),
      (1, "sachin", 2010, 14, 23, 1562, 7),
      (2, "virat", 2011, 5, 9, 202, 0),
      (2, "virat", 2012, 9, 16, 689, 3),
      (2, "virat", 2016, 12, 18, 1215, 4),
      (2, "virat", 2017, 10, 16, 1059, 5)))

  val odiRDD = sc.parallelize(List(
    (1, 2000, "sachin", 34, 34, 1328, 3),
    (1, 2001, "sachin", 17, 16, 904, 4),
    (1, 2010, "sachin", 2, 2, 1, 204),
    (1, 2011, "sachin", 11, 11, 513, 2),
    (2, 2008, "virat", 5, 5, 159, 0),
    (2, 2009, "virat", 10, 8, 325, 1),
    (2, 2016, "virat", 10, 10, 739, 3),
    (2, 2017, "virat", 26, 26, 1460, 6)))

  // Dataset operations
  case class PlayerStats(id: Int, year: Int, fname: String, matches: Int,
                         innings: Int, runs: Int, centuries: Int)

  val odiDS = odiRDD.map {
    case (id, year, fname, matches, innings, runs, centuries) =>
      PlayerStats(id, year, fname, matches, innings, runs, centuries)
  }.toDS
  odiDS.printSchema

  val testDS = testRDD.map {
    case (id, fname, year, matches, innings, runs, centuries) =>
      PlayerStats(id, year, fname, matches, innings, runs, centuries)
  }.toDS
  testDS.printSchema

  println("\nJoining datasets using the implicit inner join on two columns")
  testDS.join(odiDS, Seq("id", "year")).show

  println("\nJoinwith on Datasets and picking values from the left and right rows")
  testDS.as("t").joinWith(odiDS.as("o"), $"t.id" === $"o.id"
    && $"t.year" === $"o.year").map {
    case (t, o) => (t.id, t.year, t.matches, o.matches)
  }.show

  val tds = testDS.as("t").joinWith(odiDS.as("o"), $"t.id"
    === $"o.id" && $"t.year" === $"o.year").map {
    case (t, o) => (t.id, t.year, t.matches, o.matches)
  }
  tds.show

  case class TOMatches(id: Int, year: Int, tmatches: Int, omatches: Int)
  println("Mapping select columns from dataset join to a case class to have " +
    " a dataset with a schema")
  val tdsWithSchema: Dataset[TOMatches] = testDS.as("t").joinWith(
    odiDS.as("o"), $"t.id" === $"o.id" && $"t.year" === $"o.year").map {
      case (t, o) => TOMatches(t.id, t.year, t.matches, o.matches)
    }
  tdsWithSchema.show
}
