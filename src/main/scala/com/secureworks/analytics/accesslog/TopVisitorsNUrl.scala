package com.secureworks.analytics.accesslog

import com.secureworks.analytics.utils.Log
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

object TopVisitorsNUrl {

  val log: Logger = Log.getLogger(this.getClass.getName)

  def main(args: Array[String]): Unit = {
    val param = Param().parse(args)
    val rdd = param.spark.sparkContext.textFile(param.inputPath)
    val count = rdd.count()
    log.info("Count -> " + count)
  }
}

case class Param(
  inputPath: String = null,
  spark: SparkSession = null){

  val log: Logger = Log.getLogger(this.getClass.getName)
  val appName: String = "TopVisitorsNUrl"

  def parse(args: Array[String], sparkLocal: Boolean = false): Param = {
    val parser =
      new scopt.OptionParser[Param](appName) {
        opt[String]("inputPath").required().action { (x, c) =>
          c.copy(inputPath = x)
        }
      }
    parser.parse(args, Param()) match {
      case Some(param) =>
        param.copy(spark = getSparkSession(sparkLocal))
      case _ =>
        throw new Exception("Bad arguments")
    }
  }

  private def getSparkSession(sparkLocal: Boolean): SparkSession = {
    val builder = sparkLocal match {
      case true => SparkSession.builder.master("local[*]")
      case false => SparkSession.builder()
    }
    val ss = builder.appName(appName)
      .getOrCreate()
    log.info("Spark session created")
    ss
  }

}
