package com.secureworks.analytics.accesslog

import java.text.SimpleDateFormat

import com.secureworks.analytics.utils.Log
import org.apache.commons.net.ftp.FTPClient
import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode, SparkSession}
import org.apache.spark.sql.functions._

object TopVisitorsNUrl {

  val log: Logger = Log.getLogger(this.getClass.getName)

  /**
   * The starting point...
   * @param args User arguments
   */
  def main(args: Array[String]): Unit = {
    val param = Param().parse(args).setSparkSession()
    createTable(param.dbNtable, param.spark, param.outputPath)
    val rdd = param.spark.sparkContext.textFile(param.inputPath)
      .repartition(param.partitionCount)
    val (validDataRdd, invalidCount) = parseData(rdd)
    val topVisitorsDf = getTopN(validDataRdd, param.spark,
      "visitor", param.topN)
    val topURLsDf = getTopN(validDataRdd, param.spark,
      "url", param.topN)
    storeResult(topVisitorsDf.union(topURLsDf), param.dbNtable)
  }

  /**
   * Write the result to Hive table
   * @param result DataFrame containing the result
   * @param dbNtable Table in which the result needs to be stored
   */
  def storeResult(result: DataFrame, dbNtable: String): Unit = {
    import result.sparkSession.implicits._
    log.info("Schema -> " + result.schema.treeString)
    result
      .select($"count", $"value", $"rnk", $"dt", $"sort_col")
      .write.mode(SaveMode.Overwrite)
      .insertInto(dbNtable)
  }

  /**
   * DDL to create database & table in Hive
   * @param dbNtable <Database>.<Table>
   * @param spark
   */
  def createTable(dbNtable: String, spark: SparkSession, path: String): Unit = {
    val db = dbNtable.split("[.]{1}")(0)
    if(!spark.catalog.databaseExists(db)) {
      val ddlDb = s"CREATE DATABASE IF NOT EXISTS ${db}"
      log.info("ddlDb -> " + ddlDb)
      spark.sql(ddlDb)
    }
    if(!spark.catalog.tableExists(dbNtable)) {
      val ddlTable =
        s"""CREATE EXTERNAL TABLE IF NOT EXISTS ${dbNtable} (
           |count INT COMMENT 'No of visits',
           |value STRING COMMENT 'Value of the sorting column',
           |rnk INT COMMENT 'Top N rank')
           |PARTITIONED BY (dt DATE, sort_col STRING)
           |STORED AS PARQUET
           |TBLPROPERTIES ("parquet.compress"="SNAPPY")
           |LOCATION '${path}'
           |""".stripMargin
      log.info("ddlTable -> " + ddlTable)
      spark.sql(ddlTable)
    }
  }

  /**
   * Computes Top N for a specific column in the given RDD
   * @param rdd   Input RDD
   * @param spark -
   * @param colName Column in RDD for which Top N will be calculated
   * @param topN Top N limit
   * @return Resultant DataFrame containing Top N values
   */
  def getTopN(rdd: RDD[AccessInfo], spark: SparkSession, colName: String,
    topN: Int): DataFrame = {
    import spark.implicits._
    // Window function to calculate topN
    val windowFn = Window
      .partitionBy($"dt")
      .orderBy($"count".desc)
    val df = rdd.toDF
    val res = df.groupBy(col(colName), col("dt"))
      .count
      .withColumn("rnk", dense_rank() over windowFn)
      .where(s"rnk >= 1 and rnk <= $topN")
      .withColumn("sort_col", lit(colName))
      .withColumnRenamed(colName, "value")
    res
  }

  /**
   * Converts each line from input data into AccessInfo object
   * @param rdd RDD containing input data
   * @return Tuple._1 containing valid lines (which conforms to standard format)
   *         Tuple._2 which contains the no of invalid lines
   */
  def parseData(rdd: RDD[String]): (RDD[AccessInfo], Long) = {
    val mapped: RDD[AccessInfo] = rdd.map(splitLine(_)).cache
    val validData: RDD[AccessInfo] = mapped.filter(_ != null)
    val invalidCount: Long = mapped.filter(_ == null).count
    (validData, invalidCount)
  }

  /**
   * Parse Single line of input into AccessInfo
   * @param line Line containing access information
   * @return Parsed Data
   */
  def splitLine(line: String): AccessInfo = {
    // Ex: 199.72.81.55- - [01/Jul/1995:00:00:01 -0400] "GET /history/apollo/ HTTP/1.0" 200 6245
    val rgx = """^(.*)- - \[(.*)\]\s+"([^\s]+) ([^\s]+) ([^\s]+)" (\d+) (\d+)$""".r
    line match {
      case rgx(host, dTime, httpMethod, url, version, httpStatus, dataSize) =>
        getSqlDate(dTime) match {
          case date: java.sql.Date =>
            AccessInfo(visitor = host.trim, dTime = dTime.trim, httpMethod = httpMethod.trim,
              url = url.trim, version = version.trim, httpStatus = httpStatus.trim,
              dataSize = dataSize.trim, dt = date)
          case _ =>
            null
        }
      case _ =>
        null
    }
  }

  /**
   * Convert data from String to java.sql.Date type
   * @param dTime Data time in String
   * @return Date
   */
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





