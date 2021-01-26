package optimizationsandfindings

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object DataFramePlans extends App {
  val spark = SparkSession.builder().appName("QueryPlans")
    .master("local")
    .getOrCreate()

  val sc = spark.sparkContext
  sc.setLogLevel("ERROR")

  import spark.implicits._

  val ds1 = spark.range(1, 10000000)
  val ds2 = spark.range(2, 20000000)
  val ds3 = ds1.repartition(5)
  val ds4 = ds2.repartition(7)
  ds4.join(ds3, "id").explain()
  val ds5 = ds4.join(ds3, "id")
  ds5.agg(sum("id").as("sumofids")).explain()
  /*
  == Physical Plan ==
*(6) HashAggregate(keys=[], functions=[sum(id#20L)])
+- Exchange SinglePartition, true, [id=#145]
   +- *(5) HashAggregate(keys=[], functions=[partial_sum(id#20L)])
      +- *(5) Project [id#20L]
         +- *(5) SortMergeJoin [id#20L], [id#18L], Inner
            :- *(2) Sort [id#20L ASC NULLS FIRST], false, 0
            :  +- Exchange hashpartitioning(id#20L, 200), true, [id=#129]
            :     +- Exchange RoundRobinPartitioning(7), false, [id=#128]
            :        +- *(1) Range (2, 20000000, step=1, splits=1)
            +- *(4) Sort [id#18L ASC NULLS FIRST], false, 0
               +- Exchange hashpartitioning(id#18L, 200), true, [id=#136]
                  +- Exchange RoundRobinPartitioning(5), false, [id=#135]
                     +- *(3) Range (1, 10000000, step=1, splits=1)
   */

  val ardd = sc.parallelize(1 to 100)
  val adf = ardd.toDF
  adf.createOrReplaceTempView("atbl")
  spark.sql("select count(*) from atbl").explain

  /*
   == Physical Plan ==
    *(2) HashAggregate(keys=[], functions=[count(1)])
  +- Exchange SinglePartition, true, [id=#206]
  +- *(1) HashAggregate(keys=[], functions=[partial_count(1)])
  +- *(1) SerializeFromObject
    +- Scan[obj#35]
  rdd to dataframe conversion - cost to be paid
   */

  val ds = spark.range(10)
  val dsMapped = ds.map(_ * 5)
  ds.selectExpr("id * 5 as id5").selectExpr("count(*) as dsc").explain
  /*
  == Physical Plan ==
*(1) HashAggregate(keys=[], functions=[count(1)])
+- *(1) HashAggregate(keys=[], functions=[partial_count(1)])
   +- *(1) Project
      +- *(1) Range (0, 10, step=1, splits=1)
   */
  dsMapped.selectExpr("count(*) as dsc").explain
  /*
  == Physical Plan ==
*(1) HashAggregate(keys=[], functions=[count(1)])
+- *(1) HashAggregate(keys=[], functions=[partial_count(1)])
   +- *(1) SerializeFromObject
      +- *(1) MapElements ammonite.$sess.cmd47$$$Lambda$4527/1202480472@51623da1, obj#170: bigint
         +- *(1) DeserializeToObject staticinvoke(class java.lang.Long, ObjectType(class java.lang.Long), valueOf, id#166L, true, false), obj#169: java.lang.Long
            +- *(1) Range (0, 10, step=1, splits=1)
   */
}
