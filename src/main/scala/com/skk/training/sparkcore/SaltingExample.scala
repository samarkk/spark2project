package com.skk.training.sparkcore
import org.apache.spark.sql.SparkSession
import scala.util.Random
import org.apache.spark.rdd.RDD

object SaltingExample extends App {

  val spark = SparkSession.builder().master("local[*]").getOrCreate()
  val sc = spark.sparkContext
  sc.setLogLevel("ERROR")

  val arand = new Random
  // lets create some randomized data
  // (playerid, (year, runs, catches))
  val playerList = for (x <- 1 to 100000) yield {
    val idx = arand.nextInt(100)
    ("plr" + idx, (2009 + arand.nextInt(10), arand.nextInt(200), arand.nextInt(5)))
  }

  // lets repliicate the list and create a RDD
  val playerListReplicated = List.fill(10)(playerList).flatMap(x => x)
  val playerRDD = sc.parallelize(playerListReplicated)

  // handling skew - salting key
  // for illustration purposes lets salt keys for player 1
  val playerRDDSaltedForKeyPlr1 = playerRDD.filter(_._1 == "plr1").map { x =>
    {
      import scala.util.Random
      val arand = new Random
      val intForSaltedKey = 100 + arand.nextInt(10)
      ("plr" + intForSaltedKey, x._2)
    }
  }

  // subtract salted key to get rdd without it
  val playerRDDWithoutPlr1Key = playerRDD.filter(_._1 != "plr1")
  // check the sum of count of the two divided rdds equals that of the original rdd
  assert(
    playerRDDWithoutPlr1Key.count + playerRDDSaltedForKeyPlr1.count == playerRDD.count,
    "check salting and filter operation - something wrong there")

  // helper function to create player key and runs tuple and reduce it by key
  def reduceCalculateRuns(rdd: RDD[(String, (Int, Int, Int))]) =
    rdd.map(x => (x._1, x._2._2)).reduceByKey(_ + _)

  // verify that salted op is working properly
  // first get total runs for player 1 from salted op and then directly
  val player1RunsFromSaltedOp = reduceCalculateRuns(playerRDDSaltedForKeyPlr1).map(_._2).sum
  val player1RunsDirect = playerRDD.filter(_._1 == "plr1").map(_._2._2).sum

  // print and assert that the two match
  println("player 1 runs from direct op: " + player1RunsDirect + " , and from salted op: " +
    player1RunsFromSaltedOp)
  assert(
    player1RunsFromSaltedOp == player1RunsDirect,
    "salting operation has problems - check")

  // get the composite rdd unionizing the two ops
  val playerRuns = reduceCalculateRuns(playerRDDWithoutPlr1Key).union(
    reduceCalculateRuns(playerRDDSaltedForKeyPlr1).map(x => ("plr1", x._2)))

  // check again - that aggregates for plr1 from salted and direct ops are equal
  assert(playerRuns.filter(_._1 == "plr1").map(_._2).sum == player1RunsDirect)
}
