package com.secureworks.analytics.accesslog

/**
 * Parsed data is stored in this object
 */
case class AccessInfo(
   visitor: String,     // Ex: unicomp6.unicomp.net
   dTime: String,       // 01/Jul/1995:00:00:06 -0400
   httpMethod: String,  // GET
   url: String,         // /shuttle/countdown/
   version: String,     // HTTP/1.0
   httpStatus: String,  // 200
   dataSize: String,    // 3985
   dt: java.sql.Date    // 1995-07-01
)
