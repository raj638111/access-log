package com.secureworks.analytics.accesslog

import java.text.SimpleDateFormat

import com.secureworks.analytics.utils.Log
import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode, SparkSession}
import org.apache.spark.sql.functions._

object TopVisitorsNUrl {

  val log: Logger = Log.getLogger(this.getClass.getName)

  def main(args: Array[String]): Unit = {
    val param = Param().parse(args)
    val rdd = param.spark.sparkContext
      .textFile(param.inputPath).repartition(param.partitionCount)
    val (validDataRdd, invalidCount) = parseData(rdd)
    val topVisitorsDf = getTopN(validDataRdd, param.spark,
      "host", param.topN)
    val topURLsDf = getTopN(validDataRdd, param.spark,
      "url", param.topN)
    createTable(param.dbNtable, param.spark)
    storeResult(topVisitorsDf.union(topURLsDf), param.dbNtable)
  }

  def storeResult(result: DataFrame, dbNtable: String): Unit = {
    import result.sparkSession.implicits._
    log.info("Schema -> " + result.schema.treeString)
    result
      .select($"count", $"value", $"dt", $"sort_col")
      .write.mode(SaveMode.Overwrite)
      .insertInto(dbNtable)
  }

  def createTable(dbNtable: String, spark: SparkSession): Unit = {
    val db = dbNtable.split("[.]{1}")(0)
    val ddlDb = s"CREATE DATABASE IF NOT EXISTS ${db}"
    log.info("ddlDb -> " + ddlDb)
    spark.sql(ddlDb)
    val ddlTable =
      s"""CREATE TABLE IF NOT EXISTS ${dbNtable} (
         |count INT COMMENT 'No of visits',
         |value STRING COMMENT 'Value of the sorting column')
         |PARTITIONED BY (dt DATE, sort_col STRING)
         |STORED AS PARQUET
         |TBLPROPERTIES ("parquet.compress"="SNAPPY")
         |""".stripMargin
    log.info("ddlTable -> " + ddlTable)
    spark.sql(ddlTable)
  }

  def getTopN(rdd: RDD[AccessInfo], spark: SparkSession, colName: String,
    topN: Int): DataFrame = {
    import spark.implicits._
    val windowFn = Window
      .partitionBy($"dt")
      .orderBy($"count".desc)

    val df = rdd.toDF
    val res = df.groupBy(col(colName), col("dt"))
      .count
      .withColumn("rnk", dense_rank() over windowFn)
      .where(s"rnk >= 1 and rnk <= $topN")
      .drop("rnk")
      .withColumn("sort_col", lit(colName))
      .withColumnRenamed(colName, "value")
    res
  }

  def parseData(rdd: RDD[String]): (RDD[AccessInfo], Long) = {
    val mapped: RDD[AccessInfo] = rdd.map(splitLine(_)).cache
    val validData: RDD[AccessInfo] = mapped.filter(_ != null)
    val invalidCount: Long = mapped.filter(_ == null).count
    (validData, invalidCount)
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
              dataSize = dataSize.trim, dt = date)
          case _ =>
            null
        }
      case _ =>
        null
    }
  }

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
   dt: java.sql.Date)

case class Param(
  inputPath: String = null,
  partitionCount: Int = 8,
  topN: Int = 10,
  spark: SparkSession = null,
  dbNtable: String = "demo.test"){

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
        opt[Int]("topN").optional().action { (x, c) =>
          c.copy(topN = x)
        }
        opt[Int]("dbNtable").optional().action { (x, c) =>
          c.copy(topN = x)
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
      .config("spark.hadoop.hive.exec.dynamic.partition", "true")
      .config("spark.hadoop.hive.exec.dynamic.partition.mode", "nonstrict")
      .enableHiveSupport()
      .getOrCreate()
    log.info("Spark session created")
    ss
  }

}


/*

spark-submit --master local[*] \
--executor-memory 2G --driver-memory 2G \
--class com.secureworks.analytics.accesslog.TopVisitorsNUrl \
./target/scala-2.11/access-log-analytics-assembly-0.1.0-SNAPSHOT.jar \
--inputPath ftp://anonymous:anonpwd@ita.ee.lbl.gov/traces/NASA_access_log_Jul95.gz

spark-submit --master local[*] \
--class com.secureworks.analytics.accesslog.TopVisitorsNUrl \
./target/scala-2.11/access-log-analytics-assembly-0.1.0-SNAPSHOT.jar \
--inputPath ftp://anonymous:anonpwd@localhost:2121/data.gz

/spark/bin/spark-submit --master local[*] \
--class com.secureworks.analytics.accesslog.TopVisitorsNUrl \
/user-jars/scala-2.11/access-log-analytics-assembly-0.1.0-SNAPSHOT.jar \
--inputPath ftp://anonymous:anonpwd@localhost:2121/data.gz

/spark/bin/spark-submit --master local[*] \
--class com.secureworks.analytics.accesslog.TopVisitorsNUrl \
/user-jars/scala-2.11/access-log-analytics-assembly-0.1.0-SNAPSHOT.jar \
--inputPath ftp://anonymous:anonpwd@localhost:2121/data.gz

/spark/bin/spark-submit --master local[*] \
--executor-memory 2G --driver-memory 2G \
--class com.secureworks.analytics.accesslog.TopVisitorsNUrl \
/user-jars/scala-2.11/access-log-analytics-assembly-0.1.0-SNAPSHOT.jar \
--inputPath ftp://anonymous:anonpwd@ita.ee.lbl.gov/traces/NASA_access_log_Jul95.gz



/spark/bin/spark-submit --master local[*] \
--class org.apache.spark.examples.HdfsTest \
/spark/examples/jars/spark-examples_2.11-2.4.5.jar

 */

