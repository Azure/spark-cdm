package com.microsoft.cdm.utils

import java.text.SimpleDateFormat
import java.util.{Locale, TimeZone}

import org.apache.commons.lang.time.DateUtils
import org.apache.spark.sql.catalyst.util.{DateFormatter, DateTimeUtils, TimestampFormatter}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

/**
  * Converts between CSV/CDM data and Spark data tpyes.
  * @param dateFormats Expected string date formats.
  * @param outputDateFormat Output date format.
  */
class DataConverter(val dateFormats: String = Constants.TIMESTAMP_FORMAT,
                    val outputDateFormat: String = Constants.TIMESTAMP_FORMAT) extends Serializable {

  val dateFormatter = new SimpleDateFormat(outputDateFormat)

  private val timestampFormatter = TimestampFormatter(Constants.SINGLE_DATE_FORMAT, TimeZone.getTimeZone("UTC"))
  private val inputDateFormatter = DateFormatter(Constants.SINGLE_DATE_FORMAT)

  var timeformat = new SimpleDateFormat(Constants.TIMESTAMP_FORMAT, Locale.US)
  var dateformat = new SimpleDateFormat(Constants.SINGLE_DATE_FORMAT)

  val toSparkType: Map[CDMDataType.Value, DataType] = Map(
    CDMDataType.int64 -> LongType,
    CDMDataType.dateTime -> DateType,
    CDMDataType.string -> StringType,
    CDMDataType.double -> DoubleType,
    CDMDataType.decimal -> DecimalType(Constants.DECIMAL_PRECISION,0),
    CDMDataType.boolean -> BooleanType,
    CDMDataType.dateTimeOffset -> TimestampType
  )

  val toCdmType: Map[DataType, CDMDataType.Value] = Map(
    LongType -> CDMDataType.int64,
    DateType -> CDMDataType.dateTime,
    StringType -> CDMDataType.string,
    DoubleType -> CDMDataType.double,
    DecimalType(Constants.DECIMAL_PRECISION,0) -> CDMDataType.decimal,
    BooleanType -> CDMDataType.boolean,
    TimestampType -> CDMDataType.dateTimeOffset
  )

  val jsonToData: Map[DataType, String => Any] = Map(
    LongType -> (x => x.toLong),
    StringType -> (x => UTF8String.fromString(x)),
    DoubleType -> (x => x.toDouble),
    DecimalType(Constants.DECIMAL_PRECISION,0) -> (x => BigDecimal(x, Constants.MATH_CONTEXT)),
    BooleanType -> (x => x.toBoolean),
//    DateType -> (x => inputDateFormatter.parse(x)),
//    TimestampType -> (x => timestampFormatter.parse(x))
    DateType -> (x => inputDateFormatter.parse(dateformat.format(timeformat.parse(x)))),
    TimestampType -> (x => timestampFormatter.parse(dateformat.format(timeformat.parse(x))))
  )

  def dataToString(data: Any, dataType: DataType): String = {
    if(data == null) {
      null
    }
    else if(dataType == DateType) {
      dateFormatter.format(data)
    }
    else {
      data.toString
    }
  }

}

