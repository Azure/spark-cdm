package com.microsoft.cdm.utils

import java.math.MathContext

/**
  * Various constants for spark-csv.
  */
object Constants {

  // TODO: ensure these match the data provided
//  val DATE_FORMATS = Array("MM/dd/yyyy", "MM/dd/yyyy hh:mm:ss a")
//  val OUTPUT_FORMAT = "MM/dd/yyyy hh:mm:ss a"

  val DECIMAL_PRECISION = 28
  val MATH_CONTEXT = new MathContext(28)

  val SINGLE_DATE_FORMAT = "yyyy-MM-dd"
  val TIMESTAMP_FORMAT = "yyyy-MM-dd'T'hh:mm:ssX" // ISO8601

}
