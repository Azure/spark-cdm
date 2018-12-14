package com.microsoft.cdm.utils

import java.text.SimpleDateFormat

import org.apache.commons.lang.time.DateUtils
import org.apache.spark.sql.types._

/**
  * Converts between CSV/CDM data and Spark data tpyes.
  * @param dateFormats Expected string date formats.
  * @param outputDateFormat Output date format.
  */
class DataConverter(val dateFormats: Array[String] = Constants.DATE_FORMATS,
                    val outputDateFormat: String = Constants.OUTPUT_FORMAT) extends Serializable {

  val dateFormatter = new SimpleDateFormat(outputDateFormat)

  val toSparkType: Map[CDMDataType.Value, DataType] = Map(
    CDMDataType.int64 -> LongType,
    CDMDataType.dateTime -> DateType,
    CDMDataType.string -> StringType,
    CDMDataType.double -> DoubleType,
    CDMDataType.decimal -> DecimalType(Constants.DECIMAL_PRECISION,0),
    CDMDataType.boolean -> BooleanType,
    CDMDataType.dateTimeOffset -> DateType
  )

  val toCdmType: Map[DataType, CDMDataType.Value] = Map(
    LongType -> CDMDataType.int64,
    DateType -> CDMDataType.dateTime,
    StringType -> CDMDataType.string,
    DoubleType -> CDMDataType.double,
    DecimalType(Constants.DECIMAL_PRECISION,0) -> CDMDataType.decimal,
    BooleanType -> CDMDataType.boolean
  )

  val jsonToData: Map[DataType, String => Any] = Map(
    LongType -> (x => x.toLong),
    StringType -> (x => x),
    DoubleType -> (x => x.toDouble),
    DecimalType(Constants.DECIMAL_PRECISION,0) -> (x => BigDecimal(x, Constants.MATH_CONTEXT)),
    BooleanType -> (x => x.toBoolean),
    DateType -> (x => new java.sql.Date(DateUtils.parseDate(x, dateFormats).getTime))
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

