package com.microsoft.cdm.write

import java.io._

import com.microsoft.cdm.utils.{ADLGen2Provider, CsvParserFactory, DataConverter}
import org.apache.spark.sql.Row
import org.apache.spark.sql.sources.v2.writer.{DataWriter, WriterCommitMessage}
import org.apache.commons.io.FilenameUtils
import org.apache.spark.sql.types.StructType

import scala.collection.JavaConversions

/**
  * Writes a single partition of CDM data to a single CSV in ADLSgen2.
  * @param outputCSVFilePath uri of the output csv.
  * @param schema schema of the data to write.
  * @param adlProvider provider for ADLSgen2 actions.
  * @param dataConverter converter between Spark and CDM data.
  */
class CDMDataWriter(var outputCSVFilePath: String,
                    var schema: StructType,
                    var adlProvider: ADLGen2Provider,
                    var dataConverter: DataConverter) extends DataWriter[Row] {

  private val stream = new ByteArrayOutputStream()
  private val writer = CsvParserFactory.buildWriter(new OutputStreamWriter(stream))

  /**
    * Called by Spark runtime. Writes a row of data to an in-memory csv file.
    * @param row row of data to write.
    */
  def write(row: Row): Unit = {
    // TODO: Univocity probably doesn't need all these array conversions
    val strings: java.util.List[String] = JavaConversions.seqAsJavaList(row.toSeq.zipWithIndex.map{ case(col, index) =>
      dataConverter.dataToString(col, schema.fields(index).dataType)
    })

    var strArray = new Array[String](strings.size)
    strArray = strings.toArray(strArray)

    // TODO: periodically dump buffer when it gets to a certain size
    writer.writeRow(strArray)
  }

  /**
    * Called by Spark runtime when all data has been written. Uploads the in-memory buffer to the output CSV file.
    * @return commit message specifying location of the output csv file.
    */
  def commit: WriterCommitMessage = {
    writer.close()
    adlProvider.uploadData(stream.toString, outputCSVFilePath)
    val name = FilenameUtils.getBaseName(outputCSVFilePath)
    new CSVCommitMessage(name=name, csvLocation=outputCSVFilePath)
  }

  /**
    * Called by spark runtime.
    */
  def abort(): Unit = {
    writer.close()
    // TODO: cleanup and data written to ADLSgen2.
    // TODO: is abort called if commit throws an exception?
  }

}
