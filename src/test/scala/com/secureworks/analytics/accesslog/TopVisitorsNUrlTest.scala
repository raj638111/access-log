package com.secureworks.analytics.accesslog

import com.secureworks.analytics.utils.Log
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.scalatest.FunSuite

class TopVisitorsNUrlTest extends FunSuite{

  val log: Logger = Log.getLogger(this.getClass.getName)
  // Setting master to local to run the unit tests
  val spark = SparkSession.builder.master("local[*]")
    .appName("test")
    .enableHiveSupport()
    .getOrCreate()
  val tablePath = s"file://${System.getProperty("user.dir")}/target/demo/test"


  /**
   * Date conversion from String to java.sql.Date
   */
  test("String to java.sql.Date conversion"){
    {
      val dTime = "11/Jul/1995:00:00:01 -0400"
      assert(TopVisitorsNUrl.getSqlDate(dTime).toString == "1995-07-11")
    }
    {
      val dTime = "11/Ju/1995:00:00:01 -0400"
      assert(TopVisitorsNUrl.getSqlDate(dTime) == null)
    }
  }

  /**
   * Parsing input line with valid format
   */
  test("Parse Single Line: Valid format"){
    val goodLine =
      """199.72.81.55 - - [01/Jul/1995:00:00:01 -0400] "GET /history/apollo/ HTTP/1.0" 200 6245"""
    assert(TopVisitorsNUrl.splitLine(goodLine) ==
      AccessInfo(visitor = "199.72.81.55", dTime = "01/Jul/1995:00:00:01 -0400",
        httpMethod = "GET", httpStatus = "200", dataSize = "6245",
        dt = TopVisitorsNUrl.getSqlDate("01/Jul/1995"), url = "/history/apollo/",
        version = "HTTP/1.0"))
  }

  /**
   * Parsing input line with Invalid format ...(-0400]"GET...)
   */
  test("Parse Single Line: Invalid format"){
    val goodLine =
      """199.72.81.55- - [01/Jul/1995:00:00:01 -0400]"GET /history/apollo/ HTTP/1.0" 200 6245"""
    assert(TopVisitorsNUrl.splitLine(goodLine) == null)
  }

  /**
   * Parsing input line with Invalid Date format ...01/Ju/1995...
   */
  test("Parse Single Line: Invalid Date format"){
    val goodLine =
      """199.72.81.55- - [01/Ju/1995:00:00:01 -0400] "GET /history/apollo/ HTTP/1.0" 200 6245"""
    assert(TopVisitorsNUrl.splitLine(goodLine) == null)
  }

  /*
   * Parse input with all records being valid
   */
  test("Parse Input Data (All valid data)"){
    val rdd = spark.sparkContext.parallelize(sampleData)
    val (validDataRdd, invalidCount) = TopVisitorsNUrl.parseData(rdd)
    assert(invalidCount == 0)
  }

  /**
   * Parse input with one of the record being invalid ...01/Ju/1995...
   */
  test("Parse Input Data (One Invalid line (Bad date format))"){
    sampleData :+ """unicomp6.unicomp.net - - [01/Ju/1995:00:00:14 -0400] "GET """ +
      """/images/KSC-logosmall.gif HTTP/1.0" 200 1204""" match {
      case sampleData =>
        val rdd = spark.sparkContext.parallelize(sampleData)
        val (validDataRdd, invalidCount) = TopVisitorsNUrl.parseData(rdd)
        assert(invalidCount == 1)
    }
  }

  /**
   * Parse input with one of the record being invalid ...01/Ju/1995...
   */
  test("Parse Data (One Invalid line (Invalid line format))"){
    sampleData :+ """unicomp6.unicomp.net- - [01/Jul/1995:00:00:14 -04c""" +
      """/images/KSC-logosmall.gif HTTP/1.0" 200 1204""" match {
      case sampleData =>
        val rdd = spark.sparkContext.parallelize(sampleData)
        val (validDataRdd, invalidCount) = TopVisitorsNUrl.parseData(rdd)
        assert(invalidCount == 1)
    }
  }

  /**
   * Get Top 3 visitors for all the date
   */
  test("Top 3 Visitors") {
    var str =
      """
        |199.72.81.55 - - [01/Jul/1995:00:00:01 -0400]	 "GET /history/apollo/ HTTP/1.0" 200 6245
        |unicomp6.unicomp.net - - [01/Jul/1995:00:00:06 -0400] "GET /shuttle/countdown/ HTTP/1.0" 200 3985
        |199.120.110.21 - - [01/Jul/1995:00:00:09 -0400] "GET /shuttle/missions/sts-73/mission-sts-73.html HTTP/1.0" 200 4085
        |burger.letters.com - - [01/Jul/1995:00:00:11 -0400] "GET /shuttle/countdown/liftoff.html HTTP/1.0" 304 0
        |199.120.110.21 - - [01/Jul/1995:00:00:11 -0400] "GET /history/apollo/ HTTP/1.0" 200 4179
        |burger.letters.com - - [01/Jul/1995:00:00:12 -0400] "GET /images/NASA-logosmall.gif HTTP/1.0" 304 0
        |burger.letters.com - - [01/Jul/1995:00:00:12 -0400] "GET /shuttle/countdown/video/livevideo.gif HTTP/1.0" 200 0
        |
        |w-dyna157.net.interaccess.com - - [23/Jul/1995:15:13:22 -0400] "GET /software/winvn/winvn.html HTTP/1.0" 200 9867
        |w-dyna157.net.interaccess.com - - [23/Jul/1995:15:13:25 -0400] "GET /software/winvn/winvn.html HTTP/1.0" 200 25218
        |w-dyna157.net.interaccess.com - - [23/Jul/1995:15:13:25 -0400] "GET /software/winvn/winvn.html HTTP/1.0" 200 1414
        |w-dyna157.net.interaccess.com - - [23/Jul/1995:15:13:25 -0400] "GET /software/winvn/wvsmall.gif HTTP/1.0" 200 4441
        |w-dyna157.net.interaccess.com - - [23/Jul/1995:15:13:26 -0400] "GET /software/winvn/wvsmall.gif HTTP/1.0" 200 13372
        |www-c4.proxy.aol.com - - [23/Jul/1995:15:13:26 -0400] "GET /images/construct.gif HTTP/1.0" 200 20271
        |w-dyna157.net.interaccess.com - - [23/Jul/1995:15:13:28 -0400] "GET /images/construct.gif HTTP/1.0" 200 1204
        |www-d1.proxy.aol.com - - [23/Jul/1995:15:13:29 -0400] "GET /history/apollo/pad-abort-test-1/pad-abort-test-1.html HTTP/1.0" 200 1290
        |
        |""".stripMargin
    val rdd = spark.sparkContext.parallelize(str.split("\n"))
    val (parsedRdd, _) = TopVisitorsNUrl.parseData(rdd)

    { // Assert top 3 Visitors
      val df = TopVisitorsNUrl.getTopN(parsedRdd, spark, "visitor", 3)
      val row = df.where("count = 3").collect()(0)
      assert(row.getAs[String]("sort_col") == "visitor")
      assert(row.getAs[String]("value") == "burger.letters.com")
      assert(row.getAs[Long]("count") == 3)
      assert(df.count == 7)

    }

    { // Assert top 3 URL's
      val df = TopVisitorsNUrl.getTopN(parsedRdd, spark, "url", 3)
      val row = df.where("count = 3").collect()(0)
      assert(row.getAs[String]("sort_col") == "url")
      assert(row.getAs[String]("value") == "/software/winvn/winvn.html")
      assert(row.getAs[Long]("count") == 3)
      assert(df.count == 10)
    }
  }

  test("User argument parsing"){
    val param = Param().parse(args())
    assert(param.inputPath ==
      "ftp://anonymous:anonpwd@ita.ee.lbl.gov/traces/NASA_access_log_Jul95.gz")
    assert(param.partitionCount == 8)
    assert(param.topN == 3)
    assert(param.dbNtable == "demo.test")
    assert(param.outputPath == tablePath)
  }

  def args(): Array[String] = {
    Array("--inputPath",
      """ftp://anonymous:anonpwd@ita.ee.lbl.gov/traces/NASA_access_log_Jul95.gz""",
      "--partitionCount", "8",
      "--topN", "3",
      "--dbNtable", "demo.test",
      "--outputPath", s"${tablePath}")
  }

  def sampleData(): Array[String] = {
    Array(
      """199.72.81.55 - - [01/Jul/1995:00:00:01 -0400] "GET /history/apollo/ HTTP/1.0" 200 6245""",
      """unicomp6.unicomp.net - - [01/Jul/1995:00:00:06 -0400] "GET /shuttle/countdown/ HTTP/1.0" 200 3985""",
      """199.120.110.21 - - [01/Jul/1995:00:00:09 -0400] "GET /shuttle/missions/sts-73/mission-sts-73.html HTTP/1.0" 200 4085""",
      """burger.letters.com - - [01/Jul/1995:00:00:11 -0400] "GET /shuttle/countdown/liftoff.html HTTP/1.0" 304 0""")
  }

}

