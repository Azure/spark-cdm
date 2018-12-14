package com.microsoft.cdm.read

import com.microsoft.cdm.utils.{ADLGen2Provider, DataConverter}
import org.apache.spark.sql.Row
import org.apache.spark.sql.sources.v2.reader.DataReaderFactory
import org.apache.spark.sql.types.StructType

/**
  * Factory class for creating a CDMDataReader responsible for reading a single partition of CDM data.
  * @param remoteCSVPath ADLSgen2 URI of partition data in CSV format.
  * @param schema Spark schema of the data in the CSV file
  * @param adlProvider Provider for ADLSgen2 data
  * @param dataConverter Converts CSV data into types according to schema
  */
class CDMDataReaderFactory(var csvPath: String,
                           var schema: StructType,
                           var dataConverter: DataConverter,
                           var adlProvider: ADLGen2Provider) extends DataReaderFactory[Row] {
  def createDataReader = new CDMDataReader(csvPath, schema, adlProvider, dataConverter)
}
