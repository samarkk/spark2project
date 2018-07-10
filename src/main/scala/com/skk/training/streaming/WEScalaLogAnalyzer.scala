package com.skk.training.streaming
import java.util.regex.{ Pattern, Matcher }
class WEScalaLogAnalyzer extends Serializable {
  def transformLogData(logline: String): Map[String, String] = {
    val AALP = """^(\S+) (\S+) (\S+) \[([\w:/]+\s[+\-]\d{4})\] "(\S+)\s*(\S+)\s*(\S+)\s*" (\d{3}) (\S+)"""
    val pattern = Pattern.compile(AALP)
    val matcher = pattern.matcher(logline)
    if (!matcher.find()) {
      println("Cannot parse log line " + logline)
      return Map.empty[String, String]
    }
    createDataMap(matcher)
  }
  def createDataMap(m: Matcher): Map[String, String] = {
    return Map[String, String](
      ("IP" -> m.group(1)),
      ("client" -> m.group(2)),
      ("user" -> m.group(3)),
      ("date" -> m.group(4)),
      ("method" -> m.group(5)),
      ("request" -> m.group(6)),
      ("protocol" -> m.group(7)),
      ("respCode" -> m.group(8)),
      ("size" -> m.group(9)))
  }
}
