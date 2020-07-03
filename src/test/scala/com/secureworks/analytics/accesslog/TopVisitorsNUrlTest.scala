package com.secureworks.analytics.accesslog

import java.text.SimpleDateFormat

import com.secureworks.analytics.utils.Log
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.scalatest.FunSuite

class TopVisitorsNUrlTest extends FunSuite{

  // Setting master to local to run the unit tests
  val spark = SparkSession.builder.master("local[*]")
    .appName("test")
    .getOrCreate()
  val log: Logger = Log.getLogger(this.getClass.getName)

  test("getSqlDate(): String to java.sql.Date conversion"){
    {
      val dTime = "11/Jul/1995:00:00:01 -0400"
      assert(TopVisitorsNUrl.getSqlDate(dTime).toString == "1995-07-11")
    }
    {
      val dTime = "11/Ju/1995:00:00:01 -0400"
      assert(TopVisitorsNUrl.getSqlDate(dTime) == null)
    }
  }

  test("Parse Single Line: Valid format"){
    val goodLine =
      """199.72.81.55 - - [01/Jul/1995:00:00:01 -0400] "GET /history/apollo/ HTTP/1.0" 200 6245"""
    assert(TopVisitorsNUrl.splitLine(goodLine) ==
      AccessInfo(host = "199.72.81.55", dTime = "01/Jul/1995:00:00:01 -0400",
        httpMethod = "GET", httpStatus = "200", dataSize = "6245",
        date = getDate("1995-07-01"), url = "/history/apollo/",
        version = "HTTP/1.0"))
  }

  test("Parse Single Line: Invalid format"){
    val goodLine =
      """199.72.81.55- - [01/Jul/1995:00:00:01 -0400]"GET /history/apollo/ HTTP/1.0" 200 6245"""
    assert(TopVisitorsNUrl.splitLine(goodLine) == null)
  }

  test("Parse Single Line: Invalid Date format"){
    val goodLine =
      """199.72.81.55- - [01/Ju/1995:00:00:01 -0400] "GET /history/apollo/ HTTP/1.0" 200 6245"""
    assert(TopVisitorsNUrl.splitLine(goodLine) == null)
  }

  test("Parse Input Data (All valid data)"){
    val rdd = spark.sparkContext.parallelize(sampleData)
    val (validDataRdd, validCount, invalidCount) = TopVisitorsNUrl.parseData(rdd)
    assert(validCount == 4)
    assert(invalidCount == 0)
  }

  test("Parse Input Data (One Invalid line (Bad date format))"){
    sampleData :+ """unicomp6.unicomp.net - - [01/Ju/1995:00:00:14 -0400] "GET """ +
      """/images/KSC-logosmall.gif HTTP/1.0" 200 1204""" match {
      case sampleData =>
        val rdd = spark.sparkContext.parallelize(sampleData)
        val (validDataRdd, validCount, invalidCount) = TopVisitorsNUrl.parseData(rdd)
        assert(validCount == 4)
        assert(invalidCount == 1)
    }
  }

  test("Parse Data (One Invalid line (Invalid line format))"){
    sampleData :+ """unicomp6.unicomp.net- - [01/Jul/1995:00:00:14 -0400]"GET """ +
      """/images/KSC-logosmall.gif HTTP/1.0" 200 1204""" match {
      case sampleData =>
        val rdd = spark.sparkContext.parallelize(sampleData)
        val (validDataRdd, validCount, invalidCount) = TopVisitorsNUrl.parseData(rdd)
        assert(validCount == 4)
        assert(invalidCount == 1)
    }
  }

  def sampleData(): Array[String] = {
    Array(
      """199.72.81.55 - - [01/Jul/1995:00:00:01 -0400] "GET /history/apollo/ HTTP/1.0" 200 6245""",
      """unicomp6.unicomp.net - - [01/Jul/1995:00:00:06 -0400] "GET /shuttle/countdown/ HTTP/1.0" 200 3985""",
      """199.120.110.21 - - [01/Jul/1995:00:00:09 -0400] "GET /shuttle/missions/sts-73/mission-sts-73.html HTTP/1.0" 200 4085""",
      """burger.letters.com - - [01/Jul/1995:00:00:11 -0400] "GET /shuttle/countdown/liftoff.html HTTP/1.0" 304 0""")
  }

  def getDate(dt: String): java.sql.Date = {
    val format = new SimpleDateFormat("yyyy-MM-dd")
    val parsed = format.parse(dt)
    new java.sql.Date(parsed.getTime)
  }
}
