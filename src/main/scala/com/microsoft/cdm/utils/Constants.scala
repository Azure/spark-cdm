package com.microsoft.cdm.utils

import java.math.MathContext

object Constants {

  // TODO: ensure these match the data provided
  val DATE_FORMATS = Array("MM/dd/yyyy", "MM/dd/yyyy hh:mm:ss a")
  val OUTPUT_FORMAT = "MM/dd/yyyy hh:mm:ss a"

  val DECIMAL_PRECISION = 28
  val MATH_CONTEXT = new MathContext(28)


}
