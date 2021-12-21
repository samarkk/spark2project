package optimizationsandfindings

import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SparkSession}
import scala.util.Random


object SparkORCParquetCheck extends App {
  val spark: SparkSession = SparkSession.
    builder().
    appName("ParquetORCCheck").
    master("local[*]").
    getOrCreate()

  import spark.implicits._

  val sc: SparkContext = spark.sparkContext
  sc.setLogLevel("ERROR")

  // check overall sizes achieved for numbers, strings, csv, text, json
  // for orc and parquet respectively

  // with numbers compression achieved with orc is significantly better
  val orcWriteNumbersLocation = "file:///mnt/d/tmp/orcnmbrschk"
  val prqWriteNumbersLocation = "file:///mnt/d/tmp/prqnmbrschk"

  val ds1 = spark.range(10000000)
  ds1.write.orc(orcWriteNumbersLocation)
  ds1.write.save(prqWriteNumbersLocation)


  // with strings higher compression with orc
  val orcWriteStringsLocation = "file:///mnt/d/tmp/orcstringschk"
  val prqWriteStringLocation = "file:///mnt/d/tmp/prqstringschk"

  val random = new Random()
  val strcoll = (1 to 1000000).map(_ => random.alphanumeric.take(10).mkString(""))
  val stringsDF = sc.parallelize(strcoll).toDF("words")
  stringsDF.write.orc(orcWriteStringsLocation)
  stringsDF.write.save(prqWriteStringLocation)

  // with csv, text and json - compression achieved is similar

  /////////////    CSV                 ///////////////////
  val csvDataLocation = "file:///mnt/d/findataf/201819/cm"
  val orcWriteCSVLocation = "file:///mnt/d/tmp/orccsvchk"
  val prqWriteCSVLocation = "file:///mnt/d/tmp/prqcsvchk"

  val cmdf = spark.read.option("inferSchema", true).option("header", true).
    csv(csvDataLocation)
  cmdf.drop("_c13").write.orc(orcWriteCSVLocation)
  cmdf.drop("_c13").write.save(prqWriteCSVLocation)

  /////////////     JSON               ///////////////////
  val jsonDataLocation = "file:///mnt/d/tmp/movies_mflix.json"
  val orcWriteJsonLocation = "file:///mnt/d/tmp/orcjsonchk"
  val prqWriteJSONLocation = "file:///mnt/d/tmp/prqjsonchk"

  val mvjson = spark.read.json(jsonDataLocation)
  mvjson.write.orc(orcWriteJsonLocation)
  mvjson.write.save(prqWriteJSONLocation)

  /////////////     Text               ///////////////////
  val orcWriteTextLocation = "file:///mnt/d/tmp/orctxtchk"
  val prqWriteTextLocation = "file:///mnt/d/tmp/prqtxtchk"
  val cmtxtdf = spark.read.text(csvDataLocation).
    filter("substring(value,0,6) != 'SYMBOL'")
  cmtxtdf.write.orc(orcWriteTextLocation)
  cmtxtdf.write.save(prqWriteTextLocation)
}
