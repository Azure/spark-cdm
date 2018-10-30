package com.microsoft.cdm.write

import java.io._

import com.microsoft.cdm.utils.{ADLGen2Provider, CsvParserFactory, DataConverter}
import org.apache.spark.sql.Row
import org.apache.spark.sql.sources.v2.writer.{DataWriter, WriterCommitMessage}
import org.apache.commons.io.FilenameUtils
import org.apache.spark.sql.types.StructType

import scala.collection.JavaConversions

class CDMDataWriter(var outputCSVFilePath: String,
                    var schema: StructType,
                    var adlProvider: ADLGen2Provider,
                    var dataConverter: DataConverter) extends DataWriter[Row] {

  // TODO: don't use nulls, move to option
  // TODO: periodically dump sb when it gets to a certain size

  private val stream = new ByteArrayOutputStream()
  private val writer = CsvParserFactory.buildWriter(new OutputStreamWriter(stream))

  // TODO: Univocity probably doesn't need all these array conversions
  def write(row: Row): Unit = {
    val strings: java.util.List[String] = JavaConversions.seqAsJavaList(row.toSeq.zipWithIndex.map{ case(col, index) =>
      dataConverter.dataToString(col, schema.fields(index).dataType)
    })

    var strArray = new Array[String](strings.size)
    strArray = strings.toArray(strArray)

    writer.writeRow(strArray)
  }

  def commit: WriterCommitMessage = {
    writer.close()
    adlProvider.uploadData(stream.toString, outputCSVFilePath)
    val name = FilenameUtils.getBaseName(outputCSVFilePath)
    new CSVCommitMessage(name=name, csvLocation=outputCSVFilePath)
  }

  def abort(): Unit = {}

}
