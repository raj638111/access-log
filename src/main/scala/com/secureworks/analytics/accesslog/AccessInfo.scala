package com.secureworks.analytics.accesslog

/**
 * Parsed data is stored in this object
 */
case class AccessInfo(
   visitor: String =  null,     // Ex: unicomp6.unicomp.net
   dTime: String = null,       // 01/Jul/1995:00:00:06 -0400
   httpMethod: String = null,  // GET
   url: String = null,         // /shuttle/countdown/
   httpStatus: String = null,  // 200
   dataSize: String = null,    // 3985
   dt: java.sql.Date = null,   // 1995-07-01
)
