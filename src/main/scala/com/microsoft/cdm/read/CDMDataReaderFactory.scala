package com.microsoft.cdm.read

import com.microsoft.cdm.utils.{ADLGen2Provider, DataConverter}
import org.apache.spark.sql.Row
import org.apache.spark.sql.sources.v2.reader.DataReaderFactory
import org.apache.spark.sql.types.StructType

class CDMDataReaderFactory(var csvPath: String,
                           var schema: StructType,
                           var dataConverter: DataConverter,
                           var adlProvider: ADLGen2Provider) extends DataReaderFactory[Row] {
  def createDataReader = new CDMDataReader(csvPath, schema, adlProvider, dataConverter)
}
