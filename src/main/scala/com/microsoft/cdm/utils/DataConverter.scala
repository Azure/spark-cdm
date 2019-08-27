package com.microsoft.cdm.utils

import java.text.SimpleDateFormat
import java.util.{Locale, TimeZone}

import org.apache.commons.lang.time.DateUtils
import org.apache.spark.sql.catalyst.util.{DateTimeUtils, TimestampFormatter}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

/**
  * Converts between CSV/CDM data and Spark data tpyes.
  * @param dateFormats Expected string date formats.
  * @param outputDateFormat Output date format.
  */
class DataConverter(val dateFormats: Array[String] = Constants.DATE_FORMATS,
                    val outputDateFormat: String = Constants.OUTPUT_FORMAT) extends Serializable {

  val dateFormatter = new SimpleDateFormat(outputDateFormat)

  private val timestampFormatter = TimestampFormatter("MM/dd/yyyy", TimeZone.getTimeZone("UTC"), Locale.US

  var format = new SimpleDateFormat("MM/dd/yyyy hh:mm:ss a", Locale.US)
  var format2 = new SimpleDateFormat("MM/dd/yyyy")

  val toSparkType: Map[CDMDataType.Value, DataType] = Map(
    CDMDataType.int64 -> LongType,
    CDMDataType.dateTime -> TimestampType,
    CDMDataType.string -> StringType,
    CDMDataType.double -> DoubleType,
    CDMDataType.decimal -> DecimalType(Constants.DECIMAL_PRECISION,0),
    CDMDataType.boolean -> BooleanType,
    CDMDataType.dateTimeOffset -> TimestampType
  )

  val toCdmType: Map[DataType, CDMDataType.Value] = Map(
    LongType -> CDMDataType.int64,
    TimestampType -> CDMDataType.dateTime,
    StringType -> CDMDataType.string,
    DoubleType -> CDMDataType.double,
    DecimalType(Constants.DECIMAL_PRECISION,0) -> CDMDataType.decimal,
    BooleanType -> CDMDataType.boolean
  )

  val jsonToData: Map[DataType, String => Any] = Map(
    LongType -> (x => x.toLong),
    StringType -> (x => UTF8String.fromString(x)),
    DoubleType -> (x => x.toDouble),
    DecimalType(Constants.DECIMAL_PRECISION,0) -> (x => BigDecimal(x, Constants.MATH_CONTEXT)),
    BooleanType -> (x => x.toBoolean),
//    TimestampType -> (x => DateUtils.parseDate(x, dateFormats).getTime)
    TimestampType -> (x => timestampFormatter.parse(format2.format(format.parse(x))))
  )

  def dataToString(data: Any, dataType: DataType): String = {
    if(data == null) {
      null
    }
    else if(dataType == TimestampType) {
      dateFormatter.format(data)
    }
    else {
      data.toString
    }
  }

}

