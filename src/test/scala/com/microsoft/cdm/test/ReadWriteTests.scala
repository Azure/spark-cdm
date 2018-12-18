package com.microsoft.cdm.test

import com.microsoft.cdm.utils.{AADProvider, ADLGen2Provider, CDMModel}
import org.apache.spark.sql.{Row, SparkSession}
import org.scalatest.{BeforeAndAfter, FunSuite}

import scala.util.{Random, Try}

class ReadWriteTests extends FunSuite with BeforeAndAfter {

  private val appId = sys.env("APP_ID")
  private val appKey = sys.env("APP_KEY")
  private val tenantId = sys.env("TENANT_ID")
  private val inputModel = sys.env("DEMO_INPUT_MODEL")
  private val outputTestDir = sys.env("OUTPUT_TEST_DIR")
  private val spark = SparkSession.builder().master("local").appName("DemoTest").getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")

  test("read and write basic CDM folders") {
    val entities = getEntities(inputModel)

    val outputDir = outputTestDir + "output" + Random.alphanumeric.take(5).mkString("") + "/"
    val outputModelName = Random.alphanumeric.take(5).mkString("")

    val collections: Map[String, Int] = entities map (t => t ->
      readWrite(inputModel, t, write=true, outputDir, outputModelName).length) toMap

    val outputModel = outputDir + "model.json"
    val outputEntities = getEntities(outputModel)

    assert(outputEntities.size == entities.size)

    collections.foreach{case (entity, size) =>
        assert(size == readWrite(outputModel, entity, write=false).length)
    }

    println("Done!")
  }

  private def getEntities(modelUri: String): Iterable[String] = {
    val aadProvider = new AADProvider(appId, appKey, tenantId)
    val adls = new ADLGen2Provider(aadProvider)
    val modelJson = new CDMModel(adls.getFullFile(modelUri))
    modelJson.listEntities()
  }

  private def readWrite(modelUri: String,
                        entity: String,
                        write: Boolean = true,
                        outputDirectory: String = "",
                        outputModelName: String = ""): Array[Row] = {
    println("%s : %s".format(entity, modelUri))

    val df = spark.read.format("com.microsoft.cdm")
      .option("cdmModel", modelUri)
      .option("entity", entity)
      .option("appId", appId)
      .option("appKey", appKey)
      .option("tenantId", tenantId)
      .load()
    val collection = Try(df.collect()).getOrElse(null)

    if(write) {
      df.write.format("com.microsoft.cdm")
        .option("entity", entity)
        .option("appId", appId)
        .option("appKey", appKey)
        .option("tenantId", tenantId)
        .option("cdmFolder", outputDirectory)
        .option("cdmModelName", outputModelName)
        .save()
    }

    collection
  }

}
