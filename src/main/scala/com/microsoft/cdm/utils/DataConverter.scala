package com.microsoft.cdm.utils

import java.text.SimpleDateFormat
import java.util.{Locale, TimeZone}

import org.apache.commons.lang.time.DateUtils
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

/**
  * Converts between CSV/CDM data and Spark data tpyes.
  * @param dateFormats Expected string date formats.
  * @param outputDateFormat Output date format.
  */
class DataConverter() extends Serializable {

  val dateFormatter = new SimpleDateFormat(Constants.SINGLE_DATE_FORMAT)
  val timestampFormatter = new SimpleDateFormat(Constants.TIMESTAMP_FORMAT)

  val toSparkType: Map[CDMDataType.Value, DataType] = Map(
    CDMDataType.int64 -> LongType,
    CDMDataType.dateTime -> DateType,
    CDMDataType.string -> StringType,
    CDMDataType.double -> DoubleType,
    CDMDataType.decimal -> DecimalType(Constants.DECIMAL_PRECISION,0),
    CDMDataType.boolean -> BooleanType,
    CDMDataType.dateTimeOffset -> TimestampType
  )

  val jsonToData: Map[DataType, String => Any] = Map(
    LongType -> (x => x.toLong),
    StringType -> (x => UTF8String.fromString(x)),
    DoubleType -> (x => x.toDouble),
    DecimalType(Constants.DECIMAL_PRECISION,0) -> (x => BigDecimal(x, Constants.MATH_CONTEXT)),
    BooleanType -> (x => x.toBoolean),
    DateType -> (x => dateFormatter.parse(x)),
    TimestampType -> (x => timestampFormatter.parse(x))
  )

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
    if(data == null) {
      null
    }
    else if(dataType == DateType) {
      dateFormatter.format(data)
    }
    else if(dataType == TimestampType) {
      timestampFormatter.format(data)
    }
    else {
      data.toString
    }
  }

}

