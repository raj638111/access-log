package com.secureworks.analytics.accesslog

import org.apache.spark.sql.SparkSession
import org.scalatest.FunSuite

class TopVisitorsNUrlTest extends FunSuite{

  SparkSession.builder.master("local[*]").appName("test")
    .getOrCreate()

  test("-"){
    val args = Array(
      "--inputPath",
      "ftp://anonymous:somepwd@ita.ee.lbl.gov/traces/NASA_access_log_Jul95.gz")
    TopVisitorsNUrl.main(args)
  }

}
