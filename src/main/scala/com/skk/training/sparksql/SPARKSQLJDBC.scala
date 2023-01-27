package com.skk.training.sparksql
import org.apache.spark.sql.SparkSession
import java.util.Properties

object SPARKSQLJDBC extends App {
  val spark = SparkSession.
      builder().
      appName("SparkJDBC").
      getOrCreate()

  val prop = new Properties()
  prop.put("user", "root"); prop.put("password", "ramanShastri24!");
  prop.put("driver", "com.mysql.jdbc.Driver")

  val url = "jdbc:mysql://localhost:3306/testdb"
  val password = "ramanShastri24!"
  val mockdf = spark.read.format("jdbc").
  option("url", url).
  option("dbtable", "mocktbl") .
  option("user", "root").
  option("password", password ) .
  option("driver", "com.mysql.cj.jdbc.Driver").load()

  mockdf.show()
  mockdf.printSchema()
  mockdf.rdd.glom().map(_.length).collect()

  val mockdf_part = spark.read.format("jdbc").
    option("url", url).
    option("dbtable", "mocktbl") .
    option("user", "root").
    option("password", password).
    option("driver", "com.mysql.cj.jdbc.Driver").
    option("partitionColumn", "id").option("lowerBound", 0).
    option("upperBound", 1000).
    option("numPartitions", 4).load()

  mockdf_part.show()
  mockdf.rdd.glom().map(_.length).collect()

  // the partition stride is (upberBound - lowerBound)/numPartitions
  spark.read.format("jdbc").
  option("url", url).
  option("dbtable", "mocktbl") .
  option("user", "root").
  option("password","ramanShastri24!") .
  option("driver", "com.mysql.jdbc.Driver") .option("partitionColumn", "id").
  option("lowerBound", 950).
  option("upperBound", 1000) .
  option("numPartitions", 4).load().rdd.glom().map(_.length).collect()

  val filter_query = "(select fname, lname from mocktbl where id between 10 and 20) fq"
  val pushDownDF= spark.read.jdbc("jdbc:mysql://localhost:3306/testdb", filter_query , prop)

  // predicate pushdown works only with data frames, not with sql
  pushDownDF.explain()
  mockdf.filter("id between 10 and 20").explain()

  mockdf.write.mode("append").jdbc("jdbc:mysql://localhost:3306/testdb","mocktblcp",prop)
}
