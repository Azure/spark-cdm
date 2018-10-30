package com.microsoft.fakedatasource

import org.apache.spark.sql.sources.v2._

class DefaultSource extends DataSourceV2 with ReadSupport {
  def createReader(options: DataSourceOptions) = new FakeDataSourceReader()
}
