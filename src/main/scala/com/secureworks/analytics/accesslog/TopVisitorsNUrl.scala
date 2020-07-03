package com.secureworks.analytics.accesslog

import java.text.SimpleDateFormat

import com.secureworks.analytics.utils.Log
import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions._

object TopVisitorsNUrl {

  val log: Logger = Log.getLogger(this.getClass.getName)

  def main(args: Array[String]): Unit = {
    val param = Param().parse(args)
    val rdd = param.spark.sparkContext
      .textFile(param.inputPath).repartition(param.partitionCount)
    val (validDataRdd, validCount, invalidCount) = parseData(rdd)
    //val topURLs = getTopNURLs(parsed, param.spark)
    //val topVisitors = getTopNVisitors(parsed, param.spark)
  }

  def getTopN(rdd: RDD[AccessInfo], spark: SparkSession, colName: String,
    topN: Int): DataFrame = {
    import spark.implicits._
    val df = rdd.toDF
    val res = df.groupBy(col(colName), col("date"))
      .count
      .orderBy($"count".desc)
      .limit(topN)
      .withColumn("-", lit("-"))
    res
  }

  def parseData(rdd: RDD[String]): (RDD[AccessInfo], Long, Long) = {
    val mapped: RDD[AccessInfo] = rdd.map(splitLine(_)).cache
    val validData: RDD[AccessInfo] = mapped.filter(_ != null).cache
    val validCount = validData.count
    val invalidCount: Long = mapped.filter(_ == null).count
    mapped.unpersist()
    (validData, validCount, invalidCount)
  }

  def splitLine(line: String): AccessInfo = {
    // Ex: 199.72.81.55- - [01/Jul/1995:00:00:01 -0400] "GET /history/apollo/ HTTP/1.0" 200 6245
    val rgx = """^(.*)- - \[(.*)\]\s+"([^\s]+) ([^\s]+) ([^\s]+)" (\d+) (\d+)$""".r
    line match {
      case rgx(host, dTime, httpMethod, url, version, httpStatus, dataSize) =>
        getSqlDate(dTime) match {
          case date: java.sql.Date =>
            AccessInfo(host = host.trim, dTime = dTime.trim, httpMethod = httpMethod.trim,
              url = url.trim, version = version.trim, httpStatus = httpStatus.trim,
              dataSize = dataSize.trim, date = date)
          case _ =>
            null
        }
      case _ =>
        null
    }
  }

  /*def getSqlDate(dTime: String): java.sql.Date = {
    val format = new SimpleDateFormat("dd/MMM/yyyy")
    val rgx = "^([0-9]{2}/[a-zA-Z]{3}/[0-9]{4}).*$".r
    dTime match {
      case rgx(dt) =>
        val parsed = format.parse(dt)
        new java.sql.Date(parsed.getTime)
      case _ =>
        null
    }
  }*/

  def getSqlDate(dTime: String): java.sql.Date = {
    val format = new SimpleDateFormat("dd/MMM/yyyy")
    try {
      val parsed = format.parse(dTime)
      new java.sql.Date(parsed.getTime)
    }catch {
      case ex: java.text.ParseException =>
        null
    }
  }

}

case class AccessInfo(
     host: String,
     dTime: String,
     httpMethod: String,
     url: String,
     version: String,
     httpStatus: String,
     dataSize: String,
     date: java.sql.Date)

case class Param(
  inputPath: String = null,
  partitionCount: Int = 8,
  topN: Int = 10,
  spark: SparkSession = null){

  val log: Logger = Log.getLogger(this.getClass.getName)
  val appName: String = "TopVisitorsNUrl"

  def parse(args: Array[String]): Param = {
    val parser =
      new scopt.OptionParser[Param](appName) {
        opt[String]("inputPath").required().action { (x, c) =>
          c.copy(inputPath = x)}
        opt[Int]("partitionCount").optional().action { (x, c) =>
          c.copy(partitionCount = x)
        }
      }
    parser.parse(args, Param()) match {
      case Some(param) =>
        param.copy(spark = getSparkSession())
      case _ =>
        throw new Exception("Bad arguments")
    }
  }

  def getSparkSession(): SparkSession = {
    val ss = SparkSession
      .builder()
      .appName(appName)
      .getOrCreate()
    log.info("Spark session created")
    ss
  }

}
