package com.skk.training.sparkcore
import org.apache.spark.sql.SparkSession
import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.storage.StorageLevel
import scala.reflect.ClassTag
import org.apache.spark.rdd.RDD
import java.io.ByteArrayOutputStream
import org.apache.hadoop.io.BytesWritable
import org.apache.hadoop.io.NullWritable
import org.apache.spark.rdd.RDD.rddToSequenceFileRDDFunctions

class Stock(val name: String, val exchange: String, val series: String,
            val price: Double, val open: Double, val close: Double) extends Serializable {
  override def toString = name + ", " + exchange + ", " + series + ", " +
    price + ", " + open + ", " + close
}

object KryoSerializationExample extends App {
  val sparkConf = new SparkConf
  val useKryo = false
  if (useKryo) {
    sparkConf.setMaster("local[*]").set(
      "spark.serializer",
      "org.apache.spark.serializer.KryoSerializer")
    sparkConf.set("spark.kryo.registrationRequired", "true")
    //  val kryo = new KryoSerializer(sparkConf)

    sparkConf.registerKryoClasses(Array(
      classOf[com.skk.training.sparkcore.Stock],
      classOf[Array[com.skk.training.sparkcore.Stock]],
      classOf[scala.collection.mutable.WrappedArray.ofRef[_]],
      classOf[org.apache.spark.internal.io.FileCommitProtocol$TaskCommitMessage],
      classOf[scala.collection.immutable.Set$EmptySet$]))
  } else {
    sparkConf.setMaster("local[*]")
  }
  val spark = SparkSession.builder().config(sparkConf).getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")
  spark.conf.getAll.foreach(println)
  spark.sparkContext.setLogLevel("ERROR")
  val listOfStocks = List(
    new Stock("google", "NASDAQ", "EQ", 674.3, 670.6, 690.3),
    new Stock("infy", "NSE", "EQ", 9.5, 9.7, 9.3),
    new Stock("saxobank", "LSE", "EQ", 23.3, 21.2, 28.2),
    new Stock("statebank", "BSE", "EQ", 3.1, 3.4, 3.3))
  val replicatedListOfStocks = List.fill(10)(listOfStocks).flatten
  val stocksRDD = spark.sparkContext.parallelize(replicatedListOfStocks)
  println("No of partitions of stocksRDD: " + stocksRDD.getNumPartitions)
  stocksRDD.persist(StorageLevel.MEMORY_ONLY_SER)
  println("stocks price reduced: " + stocksRDD.map(_.price).reduce(_ + _))
  println("executor memory status: " + spark.sparkContext.getExecutorMemoryStatus)
  println("memory used for storage: " + spark.sparkContext.getExecutorStorageStatus(0).memUsed)

  def saveAsObjectFile[T: ClassTag](rdd: RDD[T], path: String): Unit = {
    val kryoSerializer = new KryoSerializer(rdd.context.getConf)
    rdd.mapPartitions(iter => iter.grouped(2).map(_.toArray)).map(outputArray => {
      val kryo = kryoSerializer.newKryo()

      val bao = new ByteArrayOutputStream
      val output = kryoSerializer.newKryoOutput()
      output.setOutputStream(bao)
      kryo.writeClassAndObject(output, outputArray)
      output.close()

      val byteWritable = new BytesWritable(bao.toByteArray())
      (NullWritable.get, byteWritable)
    }).saveAsSequenceFile(path)
  }

  def readKryoObjectFile[T](sc: SparkContext, path: String)(implicit ct: ClassTag[T]): RDD[T] = {
    val kryoSerializer = new KryoSerializer(sc.getConf)
    sc.sequenceFile(path, classOf[NullWritable], classOf[BytesWritable])
      .flatMap(x => {
        val kryo = kryoSerializer.newKryo()
        val input = new com.esotericsoftware.kryo.io.Input
        input.setBuffer(x._2.getBytes)
        val data = kryo.readClassAndObject(input)
        val dataObject = data.asInstanceOf[Array[T]]
        dataObject
      })
  }

  def deleteRecursively(file: java.io.File): Unit = {
    if (file.isDirectory())
      file.listFiles.foreach(deleteRecursively)
    if (file.exists() && !file.delete())
      throw new Exception(s"unable to delete ${file.getAbsolutePath}")
  }
  deleteRecursively(new java.io.File("D:\\temp\\stockskryo"))
  saveAsObjectFile(stocksRDD, "D:\\temp\\stockskryo")
  val kryoStocksRDD = readKryoObjectFile[Stock](spark.sparkContext, "D:\\temp\\stockskryo")
  println("no of elements read from kryo object file: " + kryoStocksRDD.count())
  kryoStocksRDD.collect().foreach(println)
}
