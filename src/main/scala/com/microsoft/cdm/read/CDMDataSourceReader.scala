package com.microsoft.cdm.read

import com.microsoft.cdm.utils.{ADLGen2Provider, CDMModel, DataConverter}
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.reader.{DataSourceReader, InputPartition}
import org.apache.spark.sql.types.StructType

/**
  * DataSourceReader implementation responsible for deciding number of partitions and schema of CDM data frame to read.
  * @param modelUri ADLSgen2 URI of the model.json file to parse
  * @param entityName Entity to look at within the model.json file
  * @param dataConverter Converts between CSV data and Spark data types.
  * @param adlProvider Provides connection to ADLSgen2 storage account.
  */
class CDMDataSourceReader(val modelUri: String,
                          val entityName: String,
                          val dataConverter: DataConverter,
                          val adlProvider: ADLGen2Provider) extends DataSourceReader {

  private val modelJsonParser = new CDMModel(adlProvider.getFullFile(modelUri))

  /**
    * Called by the Spark runtime. Reads the model.json for the entity specified to determine its schema.
    * @return The schema of the entity specified within the model.json
    */
  def readSchema(): StructType = {
    modelJsonParser.schema(entityName)
  }

  /**
    * Called by the Spark runtime. Reads the model.json to find the number of data partitions for the entity specified.
    */
  def planInputPartitions: java.util.ArrayList[InputPartition[InternalRow]]= {
    val factoryList = new java.util.ArrayList[InputPartition[InternalRow]]
    modelJsonParser.partitionLocations(entityName).foreach(csvUri => {
      factoryList.add(new CDMInputPartition(csvUri, readSchema(), dataConverter, adlProvider))
    })
    factoryList
  }

}
