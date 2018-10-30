package com.microsoft.cdm.read

import com.microsoft.cdm.utils.{ADLGen2Provider, CDMModel, DataConverter}
import org.apache.spark.sql.Row
import org.apache.spark.sql.sources.v2.reader.{DataReaderFactory, DataSourceReader}
import org.apache.spark.sql.types.StructType

class CDMDataSourceReader(val modelUri: String,
                          val entityName: String,
                          val dataConverter: DataConverter,
                          val adlProvider: ADLGen2Provider) extends DataSourceReader {

  private val modelJsonParser = new CDMModel(adlProvider.getFullFile(modelUri))

  def readSchema(): StructType = {
    modelJsonParser.schema(entityName)
  }

  def createDataReaderFactories : java.util.ArrayList[DataReaderFactory[Row]]= {
    val factoryList = new java.util.ArrayList[DataReaderFactory[Row]]
    modelJsonParser.partitionLocations(entityName).foreach(csvUri => {
      factoryList.add(new CDMDataReaderFactory(csvUri, readSchema(), dataConverter, adlProvider))
    })
    factoryList
  }

}
