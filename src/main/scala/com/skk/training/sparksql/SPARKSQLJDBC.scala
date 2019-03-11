package com.skk.training.sparksql
import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.sql.SQLContext
import java.util.Properties
import org.apache.spark.sql.SaveMode

object SPARKSQLJDBC extends App {
  val sc = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("Spark_SQL_App"))
  sc.setLogLevel("WARN")
  val sqlc = new SQLContext(sc)
  import sqlc.implicits._

  val prop = new Properties()
  prop.put("user", "root"); prop.put("password", "cloudera");
  prop.put("driver", "com.mysql.jdbc.Driver")
  val mrdf = sqlc.read.jdbc("jdbc:mysql://localhost:3306/testdb", "simptable", prop)
  mrdf.collect.foreach(println)
  val mrdf_f = sqlc.read.jdbc("jdbc:mysql://localhost:3306/testdb", "simptable",
    "age", 0, 60, 2, prop)
  print(mrdf_f.collect)
  mrdf.filter(mrdf("age") > 30).show
  mrdf.filter(mrdf("age") > 30 && mrdf("fname").startsWith("a")).select("fname").show
  //  mrdf.write.jdbc("jdbc:mysql://localhost:3306/testdb", "nstable", prop)
  mrdf.write.mode(SaveMode.Append).jdbc("jdbc:mysql://localhost:3306/testdb", "nstable", prop)
}
