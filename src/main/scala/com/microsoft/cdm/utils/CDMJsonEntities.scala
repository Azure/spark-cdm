package com.microsoft.cdm.utils

import com.google.gson.JsonObject
import org.joda.time.DateTime
import org.joda.time.format.ISODateTimeFormat

/**
  * model.json attribute entity
  * @param name name of the attribute
  * @param dataType data type of the attribute
  */
class CDMAttribute(val name: String, val dataType: CDMDataType.Value) {

  def toJson: JsonObject = {
    val jsonObject = new JsonObject()
    jsonObject.addProperty("name", name)
    jsonObject.addProperty("dataType", dataType.toString)
    jsonObject
  }

}

/**
  * model.json partition entity
  * @param name name of the partition
  * @param location uri of the csv for the partition
  * @param refreshTime refresh time of the partition (in ISO8601 format)
  */
class CDMPartition(val name: String, val location: String, val refreshTime: DateTime = new DateTime()) {

  def toJson: JsonObject = {
    val fmt = ISODateTimeFormat.dateTime

    val jsonObject = new JsonObject()
    jsonObject.addProperty("name", name)
    jsonObject.addProperty("location", location)
    jsonObject.addProperty("refreshTime", fmt.print(refreshTime))
    jsonObject
  }

}
