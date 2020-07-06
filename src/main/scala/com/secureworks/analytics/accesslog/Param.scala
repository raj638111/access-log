package com.secureworks.analytics.accesslog

import com.secureworks.analytics.utils.Log
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

/**
 * Command line arguments
 */
case class Param(
  inputPath: String = null,       // ftp://anonymous:anonpwd@ita.ee.lbl.gov/traces/
                                  //  NASA_access_log_Jul95.gz
  partitionCount: Int = 8,        // Repartition count
  topN: Int = 10,                 // Top N limit
  spark: SparkSession = null,
  dbNtable: String = "demo.test", // External hive table to store result
  outputPath: String = null)      // Path for external hive table
{

  val log: Logger = Log.getLogger(this.getClass.getName)
  val appName: String = "TopVisitorsNUrl"

  /**
   * Parse command line arguments provided by the user
   * @param args Command line argument
   * @return Object containing command line arguments
   */
  def parse(args: Array[String]): Param = {
    val parser =
      new scopt.OptionParser[Param](appName) {
        opt[String]("inputPath").required().action { (x, c) =>
          c.copy(inputPath = x)}
        opt[Int]("partitionCount").required().action { (x, c) =>
          c.copy(partitionCount = x)
        }
        opt[Int]("topN").required().action { (x, c) =>
          c.copy(topN = x)
        }
        opt[String]("dbNtable").required().action { (x, c) =>
          c.copy(dbNtable = x)
        }
        opt[String]("outputPath").required().action { (x, c) =>
          c.copy(outputPath = x)
        }
      }
    parser.parse(args, Param()) match {
      case Some(param) => param
      case _ =>
        throw new Exception("Bad arguments")
    }
  }

  /**
   * Get Spark Session
   * @return SparkSession
   */
  def setSparkSession(): Param = {
    val spark = SparkSession
      .builder()
      .appName(appName)
      .config("spark.fs.ftp.data.connection.mode", 2)
      .config("spark.hadoop.hive.exec.dynamic.partition", "true")
      .config("spark.hadoop.hive.exec.dynamic.partition.mode", "nonstrict")
      .enableHiveSupport()
      .getOrCreate()
    log.info("Spark session created")
    this.copy(spark = spark)
  }

}
