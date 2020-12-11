package sparksql
import java.util.Properties

import org.apache.spark.sql.{DataFrame, SparkSession}

object DB2Coonecion {
  val spark: SparkSession = SparkSession.builder().appName("DB2JdbcConnecrion")
    .master("local[2").getOrCreate()
  val props = new java.util.Properties
  props.put("driver", "com.ibm.db2.jcc.DB2Driver")
  props.put("user", "db2inst1")
  props.put("password", "samar")
  val url = "jdbc:db2://localhost:50000/mydb"
  val mockdf: DataFrame = spark.read.jdbc(url, "mocktbl", props)
  mockdf.show
  val mockdfn = spark.read.jdbc(url, "mocktbl", "id", 1, 100, 20, props)

  // the lower bound and upper bound are going to be used to decide the partition stride
  // in this case we have partitions from 0 to 99, from

  // if we use values 1, 100, 5 then we have fiver partitions
  // first four are of 20 each and the fifth one is 920
  // for 1, 200, 5 4 are of 40 and one is of 840
  // for 1, 1000, 5, each ppartition is of 200
  spark.read.jdbc(url, "mocktbl", "id", 0, 1000, 10, props).rdd
    .mapPartitionsWithIndex((idx, iter) =>
      iter.toList.map(x => (x.getAs[Int]("ID"), idx)).iterator)
    .collect.groupBy(x => x._2).map(x => (x._1, x._2.size))

  mockdf.write.jdbc(url, "mocktblcp", props)
  mockdf.write.mode(org.apache.spark.sql.SaveMode.Append).jdbc(url, "mocktblcp", props)
  mockdf.write.mode("append").jdbc(url, "mocktblcp", props)

}
