package com.microsoft.cdm.utils

import com.google.gson._
import org.apache.spark.sql.types._

import scala.collection.JavaConverters._

// TODO: should probably make this a proper model interface

class CDMModel(jsonString: String) {

  private val modelJson: JsonObject = new JsonParser().parse(jsonString).getAsJsonObject
  private val entities: JsonArray = modelJson.getAsJsonArray("entities")

  private def getEntity(entityName: String): JsonObject = {
    entities.asScala.find(_.getAsJsonObject.get("name").getAsString == entityName)
      .getOrElse(throw new RuntimeException("Can't find entity " + entityName)).getAsJsonObject
  }

  def listEntities(): Iterable[String] = {
    entities.asScala.map(_.getAsJsonObject.get("name").getAsString)
  }

  def partitionLocations(entity: String): Iterator[String] = {
    val partitions = getEntity(entity).getAsJsonArray("partitions").iterator().asScala
    partitions.map(part => {
      part.getAsJsonObject.get("location").getAsString
    })
  }

  def schema(entity: String): StructType = {
    val attributes = getEntity(entity).getAsJsonArray("attributes").iterator().asScala

    StructType(attributes.map(attr => {
      val attributeJson = attr.getAsJsonObject
      val name = attributeJson.get("name").getAsString
      val dataTypeJson = attributeJson.get("dataType").getAsString
      StructField(name, new DataConverter().toSparkType(CDMDataType.withName(dataTypeJson)))
    }).toArray)
  }

  def appendOrReplaceEntity(entityName: String,
                            attributes: Seq[CDMAttribute],
                            partitions: Seq[CDMPartition]): Unit = {
    CDMModel.appendToEntities(entities, entityName, attributes, partitions)
  }

  def toJson: String = {
    new GsonBuilder().setPrettyPrinting().create().toJson(modelJson)
  }

}

object CDMModel {

  def getExistingEntity(entities: JsonArray, entityName: String): Option[JsonElement] = {
    entities.asScala.find(_.getAsJsonObject.get("name").getAsString == entityName)
  }

  def appendToEntities(entities: JsonArray,
                       entityName: String,
                       attributes: Seq[CDMAttribute],
                       partitions: Seq[CDMPartition]): Unit = {
    val element = entities.asScala.find(_.getAsJsonObject.get("name").getAsString == entityName)
    val exists = element.isDefined
    val entityJson = element.getOrElse(new JsonObject).getAsJsonObject

    if(!exists)
      entities.add(entityJson)

    entityJson.addProperty("name", entityName)
    entityJson.addProperty("description", "")
    entityJson.addProperty("$type", "LocalEntity")

    val attributesJson = new JsonArray()
    attributes.foreach{attr => attributesJson.add(attr.toJson)}
    entityJson.add("attributes", attributesJson)

    val partitionsJson = new JsonArray()
    partitions.foreach{part => partitionsJson.add(part.toJson)}
    entityJson.add("partitions", partitionsJson)
  }

  def createNewModel(name: String,
                           entityName: String,
                           attributes: Seq[CDMAttribute],
                           partitions: Seq[CDMPartition]): CDMModel = {
    val modelObject = new JsonObject()
    modelObject.addProperty("name", name)
    modelObject.addProperty("description", "")
    modelObject.addProperty("version", "1.0")

    val entitiesJson = new JsonArray()
    appendToEntities(entitiesJson, entityName, attributes, partitions)

    modelObject.add("entities", entitiesJson)

    new CDMModel(new GsonBuilder().setPrettyPrinting().create().toJson(modelObject))
  }

}
