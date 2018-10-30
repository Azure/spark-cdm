package com.microsoft.fakedatasource

import org.apache.spark.sql.Row
import org.apache.spark.sql.sources.v2.reader.{DataReader, DataReaderFactory}

class FakeDataSourceReaderFactory extends
  DataReaderFactory[Row] with DataReader[Row] {
  def createDataReader = new FakeDataSourceReaderFactory()
  val values = Array("1", "2", "3", "4", "5")

  var index = 0

  def next = index < values.length

  def get = {
    val row = Row(values(index))
    index = index + 1
    row
  }

  def close() = Unit
}
