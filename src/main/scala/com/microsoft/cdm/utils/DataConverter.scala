package com.microsoft.cdm.utils

import java.text.SimpleDateFormat
import java.util.{Locale, TimeZone}
import java.sql.Timestamp

import org.apache.commons.lang.time.DateUtils
import org.apache.spark.sql.catalyst.util.TimestampFormatter
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

/**
  * Converts between CSV/CDM data and Spark data tpyes.
  * @param dateFormats Expected string date formats.
  * @param outputDateFormat Output date format.
  */
class DataConverter() extends Serializable {

  val dateFormatter = new SimpleDateFormat(Constants.SINGLE_DATE_FORMAT)
  val timestampFormatter = TimestampFormatter(Constants.TIMESTAMP_FORMAT, TimeZone.getTimeZone("UTC"))


  val toSparkType: Map[CDMDataType.Value, DataType] = Map(
    CDMDataType.int64 -> LongType,
    CDMDataType.dateTime -> DateType,
    CDMDataType.string -> StringType,
    CDMDataType.double -> DoubleType,
    CDMDataType.decimal -> DecimalType(Constants.DECIMAL_PRECISION,0),
    CDMDataType.boolean -> BooleanType,
    CDMDataType.dateTimeOffset -> TimestampType
  )

  def jsonToData(dt: DataType, value: String): Any = {
    return dt match {
      case LongType => value.toLong
      case DoubleType => value.toDouble
      case DecimalType() => Decimal(value)
      case BooleanType => value.toBoolean
      case DateType => dateFormatter.parse(value)
      case TimestampType => timestampFormatter.parse(value)
      case _ => UTF8String.fromString(value)
    }
  }

  def toCdmType(dt: DataType): CDMDataType.Value = {
    return dt match {
      case IntegerType => CDMDataType.int64
      case LongType => CDMDataType.int64
      case DateType => CDMDataType.dateTime
      case StringType => CDMDataType.string
      case DoubleType => CDMDataType.double
      case DecimalType() => CDMDataType.decimal
      case BooleanType => CDMDataType.boolean
      case TimestampType => CDMDataType.dateTimeOffset
    }
  }  

  def dataToString(data: Any, dataType: DataType): String = {
    (dataType, data) match {
      case (_, null) => null
      case (DateType, _) => dateFormatter.format(data)
      case (TimestampType, v: Number) => timestampFormatter.format(data.asInstanceOf[Long])
      case _ => data.toString
    }
  }

}

