package com.microsoft.cdm.read

import java.io.InputStream

import com.microsoft.cdm.utils._
import com.univocity.parsers.csv.CsvParser
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.reader.InputPartitionReader
import org.apache.spark.sql.types.{StringType, StructType}

/**
  * Reads a single partition of CDM data.
  * @param remoteCSVPath ADLSgen2 URI of partition data in CSV format.
  * @param schema Spark schema of the data in the CSV file
  * @param adlProvider Provider for ADLSgen2 data
  * @param dataConverter Converts CSV data into types according to schema
  */
class CDMDataReader(var remoteCSVPath: String,
                    var schema: StructType,
                    var adlProvider: ADLGen2Provider,
                    var dataConverter: DataConverter) extends InputPartitionReader[InternalRow] {

  var parser: CsvParser = _
  var stream: InputStream = _
  var row: Array[String] = _

  /**
    * Called by the Spark runtime.
    * @return Boolean indicating whether there is any data left to read.
    */
  def next: Boolean = {
    if(parser == null) {
      stream = adlProvider.getFile(remoteCSVPath)
      parser = CsvParserFactory.build()
      parser.beginParsing(stream)
    }

    row = parser.parseNext()
    row != null
  }

  /**
    * Called by the Spark runtime if there is data left to read.
    * @return The next row of data.
    */
  def get: InternalRow = {
    val seq = row.zipWithIndex.map{ case (col, index) =>
      val dataType = schema.fields(index).dataType
      if(col == null || (col.length == 0 && dataType != StringType)) {
        null
      }
      else {
        dataConverter.jsonToData(schema.fields(index).dataType, col)
      }
    }
    InternalRow.fromSeq(seq)
  }

  /**
    * Called by the Spark runtime.
    */
  def close(): Unit = {
    stream.close()
  }

}
