package com.skk.training.sparkcore

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.storage.StorageLevel

object SparkLogProcessorKryo {
  def main(args: Array[String]) {
    val conf = new SparkConf()
    conf.setAppName("SparkLogProcessorKryo")
    conf.setMaster(args(0))
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.set("spark.kryo.registrationRequired", "true")

    case class LogRecord(ipAddress: String, clientIdentity: String, userId: String,
                         dateTime: String, method: String, endPoint: String,
                         protocol: String, responseCode: String, contentSize: Long)

    def parseApacheLogLineO(logLine: String): LogRecord = {
      val AALP = """^(\S+) (\S+) (\S+) \[([\w:/]+\s[+\-]\d{4})\] "(\S+) (\S+)\s*(\S*)\s*" (\d{3}) (\S+)""".r
      val res = AALP.findFirstMatchIn(logLine)
      if (res.isEmpty) {
        throw new RuntimeException("Cannot parse log line " + logLine)
      }
      val m = res.get
      LogRecord(m.group(1), m.group(2), m.group(3), m.group(4), m.group(5), m.group(6),
        m.group(7), m.group(8), m.group(9) match { case "-" => 0; case x => x.toLong })
    }
    def parseApacheLogLine(logLine: String): LogRecord = {
      val AALP = """^(\S+) (\S+) (\S+) \[([\w:/]+\s[+\-]\d{4})\] "(\S+) (\S+)\s*(\S*)\s*" (\d{3}) (\S+)""".r
      val res = AALP.findFirstMatchIn(logLine)
      try {
        val m = res.get
        LogRecord(m.group(1), m.group(2), m.group(3), m.group(4), m.group(5), m.group(6),
          m.group(7), m.group(8), m.group(9) match { case "-" => 0; case x => x.toLong })
      } catch {
        case ex: Exception => null
      }
    }
    conf.registerKryoClasses(Array(classOf[LogRecord], classOf[Array[LogRecord]],
      classOf[String], classOf[Array[String]],
      classOf[scala.collection.mutable.WrappedArray.ofRef[_]],
      classOf[scala.reflect.ClassTag$$anon$1],
      classOf[java.lang.Class[_]], classOf[Array[Int]]))

    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    //  val AALP = """^(\S+) (\S+) (\S+) \[([\w:/]+\s[+\-]\d{4})\] "(\S+) (\S+)\s*(\S*)\s*" (\d{3}) (\S+)""".r
    // val fileLoc = "D:/ufdata/apachelogs"
    val fileLoc = args(1)
    val logFileRDD = sc.textFile(fileLoc)
    println("No of logs " + logFileRDD.count)
    logFileRDD take 2 foreach println
    val accessLogs = sc.textFile(fileLoc).map(parseApacheLogLine).filter(_ != null)
    //  assert(accessLogs.getStorageLevel.useMemory)
    //  var accessLogs = 10
    /* ... new cell ... */
    accessLogs.persist(StorageLevel.MEMORY_ONLY_SER)
    accessLogs.first

    /* ... new cell ... */

    val content_sizes = accessLogs.map(_.contentSize).cache()
    val content_sizes_avg = content_sizes.sum / content_sizes.count
    val content_sizes_min = content_sizes.min
    val content_sizes_max = content_sizes.max
    printf(
      "Content size average: %d, Min: %d, Max: %d ",
      content_sizes_avg.toInt, content_sizes_min,
      content_sizes_max)
    //  assert(content_sizes_avg.toInt == 17531
    //    && content_sizes_min == 0 && content_sizes_max == 3421948)

    /* ... new cell ... */
    //    println("\nMemory usage for access logs: " + sc.getExecutorMemoryStatus)
    //    println("\nStorage memory used for access logs: " +
    //      sc.getExecutorStorageStatus(0).memUsed)

    val responseCodes = accessLogs.map(x => x.responseCode).map(
      (_, 1)).reduceByKey(_ + _).sortBy(_._1)
    responseCodes.collect.foreach(println)
    responseCodes.take(2).toSet
    //  assert(responseCodes.first._1 == "200" && responseCodes.first._2 == 940847)

    /* ... new cell ... */

    println("Any 20 hosts that have accessed more than 10 times:\n")
    val any20HostsMoreThan10 = accessLogs.map(
      x => (x.ipAddress, 1)).reduceByKey(_ + _).filter(_._2 > 10).sortBy(-_._2).take(20)

    //  assert(any20HostsMoreThan10(0)._1 == "edams.ksc.nasa.gov" &&
    //    any20HostsMoreThan10(0)._2 == 4034)

    /* ... new cell ... */

    // TOP TEN ENDPOINTS
    val topTenEndpoints = accessLogs.map(
      x => (x.endPoint, 1)).reduceByKey(_ + _).sortBy(-_._2).take(10)
    topTenEndpoints(0)._1
    //  assert(topTenEndpoints.toSet == Set(
    //    ("""/images/NASA-logosmall.gif""", 59737),
    //    ("""/images/KSC-logosmall.gif""", 50452),
    //    ("""/images/MOSAIC-logosmall.gif""", 43890),
    //    ("""/images/USA-logosmall.gif""", 43664),
    //    ("""/images/WORLD-logosmall.gif""", 43277),
    //    ("""/images/ksclogo-medium.gif""", 41336),
    //    ("""/ksc.html""", 28582),
    //    ("""/history/apollo/images/apollo-logo1.gif""", 26778),
    //    ("""/images/launch-logo.gif""", 24755),
    //    ("""/""", 20292)))

    /* ... new cell ... */

    // Unique host count
    val accessLogsIPAddressDistinct = accessLogs.map(_.ipAddress).distinct.count
    //  assert(accessLogsIPAddressDistinct == 54507)

    /* ... new cell ... */

    // No of unique hosts by day
    // get the unique hosts by day -
    // create a tuple of day and ip address
    // call distinct on it - group by key
    // map it to the first part - the day, second part size and sort by second part descending
    val dailyUniqueHosts = accessLogs.map(x => (x.dateTime.substring(0, 2).toInt, x.ipAddress)).
      distinct.groupByKey().map(x => (x._1, x._2.size)).sortBy(_._1.toInt)
    dailyUniqueHosts.collect.foreach(println)
    //  assert(dailyUniqueHosts.collect.toSet == Set((1, 2582), (3, 3222), (4, 4190), (5, 2502), (6, 2537),
    //    (7, 4106), (8, 4406), (9, 4317), (10, 4523), (11, 4346), (12, 2864), (13, 2650),
    //    (14, 4454), (15, 4214), (16, 4340), (17, 4385), (18, 4168), (19, 2550),
    //    (20, 2560), (21, 4134), (22, 4456)))

    /* ... new cell ... */

    // Average requests per host per day
    // first get the total number of requests for each day
    val dailyRequests = accessLogs.map(x => (x.dateTime.substring(0, 2).toInt, 1)).
      reduceByKey(_ + _)
    println("The daily requests in total")
    dailyRequests.collect.foreach(println)
    //  assert(dailyRequests.collect.toSet == Set((4, 59554), (16, 56651), (8, 60142), (12, 38070), (20, 32963),
    //    (13, 36480), (21, 55539), (1, 33996), (17, 58980), (9, 60457), (5, 31888), (22, 57758), (14, 59873), (6, 32416),
    //    (18, 56244), (10, 61245), (19, 32092), (15, 58845), (11, 61242), (3, 41387), (7, 57355)))
    // join dailyUniqueHosts with dailyRequests - the key in each case will be the day
    // the value will be a two pair of number of unique hosts and total requests
    // divide the total requests by the unique hosts to get the average daily request per host
    // using tuple notation
    println("using tuple notation")
    dailyUniqueHosts.join(dailyRequests).map(x => (x._1, x._2._2 / x._2._1)).sortBy(
      _._1).collect.foreach(println)
    println("using case notation")
    val avgHostsRequestPerDay = dailyUniqueHosts.join(dailyRequests).map {
      case (day, (hosts, requests)) => (day, (requests / hosts).toInt)
    }.sortBy(_._1).collect
    //  assert(avgHostsRequestPerDay.toSet == Set((1, 13), (3, 12), (4, 14), (5, 12), (6, 12), (7, 13),
    //    (8, 13), (9, 14), (10, 13), (11, 14), (12, 13), (13, 13), (14, 13), (15, 13), (16, 13),
    //    (17, 13), (18, 13), (19, 12), (20, 12), (21, 13), (22, 12)))

    /* ... new cell ... */

    // find out the bad records the ones which got the response code 404
    val badRecords = accessLogs.filter(_.responseCode == "404").cache
    println(badRecords.count)
    assert(badRecords.count == 6185)

    /* ... new cell ... */

    // find out the  5 most frequent bad hosts - do a count descending for the ipAddress part of the
    // bad hosts
    val frequentBadHosts = badRecords.map(x => (x.ipAddress, 1)).
      reduceByKey(_ + _).sortBy(-_._2)
    frequentBadHosts.take(5).foreach(println)
    //  assert(frequentBadHosts.take(5).toSet == Set(
    //    ("""maz3.maz.net""", 39),
    //    ("""piweba3y.prodigy.com""", 39), ("""gate.barr.com""", 38),
    //    ("""ts8-1.westwood.ts.ucla.edu""", 37), ("""nexus.mlckew.edu.au""", 37)))

    /* ... new cell ... */

    // find out the 5 most frequent endponts
    val frequentBadEndPoints = badRecords.map(x => (x.endPoint, 1)).
      reduceByKey(_ + _).sortBy(-_._2).take(5)
    frequentBadEndPoints.foreach(println)
    //  assert(frequentBadEndPoints.toSet == Set(
    //    ("""/pub/winvn/readme.txt""", 633),
    //    ("""/pub/winvn/release.txt""", 494), ("""/shuttle/missions/STS-69/mission-STS-69.html""", 431),
    //    ("""/images/nasa-logo.gif""", 319), ("""/elv/DELTA/uncons.htm""", 178)))
  }
}
