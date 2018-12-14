package com.microsoft.cdm.write

import com.microsoft.cdm.utils.{ADLGen2Provider, DataConverter}
import org.apache.spark.sql.Row
import org.apache.spark.sql.sources.v2.writer.{DataWriter, DataWriterFactory}
import org.apache.spark.sql.types.StructType

/**
  * Factory class. Creates a CDMDataWriter instance for a single partition of data to write.
  * @param adlProvider provides ADLSgen2 actions.
  * @param schema schema of data to write.
  * @param jobId write job id.
  * @param modelDirectory output model directory.
  * @param entityName output entity name.
  */
class CDMDataWriterFactory(var adlProvider: ADLGen2Provider,
                           var schema: StructType,
                           var jobId: String,
                           var modelDirectory: String,
                           var entityName: String) extends DataWriterFactory[Row] {

  // TODO: error handling. we're basically assuming successful writes. Need to add logic to remove/rewrite files on failure.

  def createDataWriter(partitionId: Int, attemptNumber: Int): DataWriter[Row] = {
    new CDMDataWriter(
      adlProvider.getFilePathForPartition(modelDirectory, entityName, partitionId),
      schema,
      adlProvider,
      new DataConverter())
  }

}
