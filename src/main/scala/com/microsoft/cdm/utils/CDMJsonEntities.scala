package com.microsoft.cdm.utils

import java.text.SimpleDateFormat
import java.util.Date

import com.google.gson.JsonObject
import org.joda.time.DateTime
import org.joda.time.format.ISODateTimeFormat

class CDMAttribute(val name: String, val dataType: CDMDataType.Value) {

  def toJson: JsonObject = {
    val jsonObject = new JsonObject()
    jsonObject.addProperty("name", name)
    jsonObject.addProperty("dataType", dataType.toString)
    jsonObject
  }

}

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
