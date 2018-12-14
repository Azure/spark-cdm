package com.microsoft.cdm.write

import org.apache.spark.sql.sources.v2.writer.WriterCommitMessage

// TODO: there's a better scala idiom for this than class
/**
  * Commit message returned from CDMDataWriter on successful write. One for each partition gets returned to
  * CDMDataSourceWriter.
  * @param name name of the partition.
  * @param csvLocation output csv file for the partition.
  */
class CSVCommitMessage(val name: String, val csvLocation: String) extends WriterCommitMessage {

}
