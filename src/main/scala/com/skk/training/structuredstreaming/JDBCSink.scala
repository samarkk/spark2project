package com.skk.training.structuredstreaming

import java.sql.Connection
import java.sql.PreparedStatement
import java.sql.DriverManager
import org.apache.spark.sql.ForeachWriter
import org.apache.spark.sql.Row
import com.mysql.jdbc.Driver

class JDBCSink(url: String, user: String, pwd: String) extends ForeachWriter[Row] {
  val driver = "com.mysql.jdbc.Driver"
  var connection: Connection = _
  var statement: java.sql.PreparedStatement = _
  val sqlToExecute = """insert into testdb.stockstats values(?, ?, ?, ?, ?, ?, ?)"""

  def open(partitionId: Long, version: Long): Boolean = {
    Class.forName(driver)
    connection = DriverManager.getConnection(url, user, pwd)
    connection.setAutoCommit(false)
    statement = connection.prepareStatement(sqlToExecute)
    true
  }

  def process(value: org.apache.spark.sql.Row): Unit = {
    statement.setString(1, value(0).toString)
    statement.setString(2, value(1).toString)
    statement.setString(3, value(2).toString)
    statement.setString(4, value(3).toString)
    statement.setString(5, value(1).toString)
    statement.setString(6, value(2).toString)
    statement.setString(7, value(3).toString)
    statement.executeUpdate()
  }

  def close(errorOrNull: Throwable): Unit = {
    connection.commit()
    connection.close
  }
}
