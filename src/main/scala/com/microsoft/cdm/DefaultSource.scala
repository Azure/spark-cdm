package com.microsoft.cdm

import java.util.Optional

import com.microsoft.cdm.read.CDMDataSourceReader
import com.microsoft.cdm.utils.{AADProvider, ADLGen2Provider, Constants, DataConverter}
import com.microsoft.cdm.write.CDMDataSourceWriter
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.sources.v2._
import org.apache.spark.sql.sources.v2.writer.DataSourceWriter
import org.apache.spark.sql.types.StructType

class DefaultSource extends DataSourceV2 with ReadSupport with WriteSupport {

  private def getDataStorage(options: DataSourceOptions): ADLGen2Provider = {
    val appId = options.get("appId").get()
    val appKey = options.get("appKey").get()
    val tenantId = options.get("tenantId").get()
    new ADLGen2Provider(new AADProvider(appId, appKey, tenantId))
  }

  // TODO: Move model processing here
  def createReader(options: DataSourceOptions): CDMDataSourceReader = {
    val modelPath = options.get("cdmModel").get()
    val entityName = options.get("entity").get()
    new CDMDataSourceReader(modelPath, entityName, new DataConverter(), getDataStorage(options))
  }

  def createWriter(jobId: String,
                   schema: StructType,
                   mode: SaveMode,
                   options: DataSourceOptions): Optional[DataSourceWriter] = {
    val modelDirectory = options.get("cdmFolder").get()
    val modelName = options.get("cdmModelName").get()
    val entity = options.get("entity").get()

    Optional.of(new CDMDataSourceWriter(jobId,
      schema, mode, getDataStorage(options), modelDirectory, modelName, entity, new DataConverter()))
  }
}
