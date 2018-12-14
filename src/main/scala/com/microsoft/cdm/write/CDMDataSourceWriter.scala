package com.microsoft.cdm.write

import com.microsoft.cdm.utils._
import org.apache.spark.sql.{Row, SaveMode}
import org.apache.spark.sql.sources.v2.writer.{DataSourceWriter, DataWriterFactory, WriterCommitMessage}
import org.apache.spark.sql.types.StructType

/**
  * DataSourceWriter implementation responsible for committing and aborting writes, as well as creating
  * DataWriterFactory instances for each partition.
  * @param jobId id of the write job.
  * @param schema schema of the data to write.
  * @param mode SaveMode specified by user. TODO: need to take this parameter into account
  * @param adlProvider provider for ADLSgen2 actions.
  * @param modelDirectory path for output directory to write to.
  * @param modelName output model name.
  * @param entityName output entity name.
  * @param dataConvert converts between Spark and CDM/CSV data.
  */
class CDMDataSourceWriter(val jobId: String,
                          val schema: StructType,
                          val mode: SaveMode,
                          val adlProvider: ADLGen2Provider,
                          val modelDirectory: String,
                          val modelName: String,
                          val entityName: String,
                          val dataConvert: DataConverter) extends DataSourceWriter {

  def createWriterFactory: DataWriterFactory[Row] = {
    new CDMDataWriterFactory(adlProvider, schema, jobId, modelDirectory, entityName)
  }

  private val createNewModel = (modelUri: String, attributes: Seq[CDMAttribute], partitions: Seq[CDMPartition]) => {
    CDMModel.createNewModel(modelName, entityName, attributes, partitions).toJson
  }

  private val appendExistingModel = (modelUri: String, attributes: Seq[CDMAttribute], partitions: Seq[CDMPartition]) => {
    val existingModel = new CDMModel(adlProvider.getFullFile(modelUri))
    existingModel.appendOrReplaceEntity(entityName, attributes, partitions)
    existingModel.toJson
  }

  def commit(messages: Array[WriterCommitMessage]): Unit = {
    val partitions = messages.map{ message =>
      val csvMsg = message.asInstanceOf[CSVCommitMessage]
      new CDMPartition(name=csvMsg.name, location=csvMsg.csvLocation)
    }

    val attributes = schema.map{ col =>
      new CDMAttribute(col.name, dataConvert.toCdmType(col.dataType))
    }

    // Check if there's an existing model in this directory to append to
    val modelUri = adlProvider.getModelJsonInDirectory(modelDirectory)
    val modelJson = (if(adlProvider.fileExists(modelUri)) appendExistingModel else createNewModel)(modelUri,
      attributes, partitions)

    adlProvider.uploadData(modelJson, modelUri)
  }

  // TODO: error handling
  def abort(messages: Array[WriterCommitMessage]): Unit = {}

}

