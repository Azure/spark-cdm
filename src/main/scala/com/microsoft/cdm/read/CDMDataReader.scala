package com.microsoft.cdm.read

import com.microsoft.cdm.utils._
import com.univocity.parsers.csv.CsvParser
import org.apache.spark.sql.Row
import org.apache.spark.sql.sources.v2.reader.DataReader
import org.apache.spark.sql.types.{StringType, StructType}

class CDMDataReader(var remoteCSVPath: String,
                    var schema: StructType,
                    var adlProvider: ADLGen2Provider,
                    var dataConverter: DataConverter) extends DataReader[Row] {

  var parser: CsvParser = _
  var row: Array[String] = _

  def next: Boolean = {
    if(parser == null) {
      parser = CsvParserFactory.build()
      parser.beginParsing(adlProvider.getFile(remoteCSVPath))
    }

    row = parser.parseNext()
    row != null
  }

  def get: Row = {
    val seq = row.zipWithIndex.map{ case (col, index) =>
      val dataType = schema.fields(index).dataType
      if(col == null || (col.length == 0 && dataType != StringType)) {
        null
      }
      else {
        dataConverter.jsonToData(schema.fields(index).dataType)(col)
      }
    }
    Row.fromSeq(seq)
  }

  def close(): Unit = {}

}
