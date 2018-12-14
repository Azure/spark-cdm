package com.microsoft.cdm.utils

/**
  * Enum containing possible data types for CDM data.
  */
object CDMDataType extends Enumeration {
  val int64, dateTime, string, double, decimal, boolean, dateTimeOffset = Value
}
