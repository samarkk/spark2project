package com.skk.training.sparksql

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.{ MutableAggregationBuffer, UserDefinedAggregateFunction }
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row

object SparkUDAFExample extends App {
  private class SumProductAF extends UserDefinedAggregateFunction {
    def inputSchema: StructType = new StructType().add("price", DoubleType).add(
      "quantity", LongType)

    def bufferSchema: StructType = new StructType().add("total", DoubleType)

    def deterministic: Boolean = true

    def dataType: DataType = DoubleType

    def initialize(buffer: MutableAggregationBuffer): Unit = {
      buffer.update(0, 0.0)
    }

    def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
      val sum = buffer.getDouble(0)
      val price = input.getDouble(0)
      val quantity = input.getLong(1)
      buffer.update(0, sum + (price * quantity))
    }

    def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
      buffer1.update(0, buffer1.getDouble(0) + buffer2.getDouble(0))
    }

    def evaluate(buffer: Row): Any = buffer.getDouble(0)
  }
  val spark = SparkSession.builder()
    .appName("SparkUDAFExample")
    .master("local[*]")
    .getOrCreate()

  val sc = spark.sparkContext
  //  println(sc.version + " , " + spark.version)
  sc.setLogLevel("ERROR")

  val testDF = spark.read.json("file:///D:/ufdata/inventory.json")
  spark.udf.register("sumproduct", new SumProductAF)
  testDF.createOrReplaceTempView("invjson")
  spark.sql("""select make, sumproduct(RetailValue, Stock)
   as inv_value_per_make from invjson
   group by make""").show
}
