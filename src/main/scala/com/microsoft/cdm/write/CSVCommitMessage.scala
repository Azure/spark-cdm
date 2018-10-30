package com.microsoft.cdm.write

import org.apache.spark.sql.sources.v2.writer.WriterCommitMessage

// TODO: there's a better scala idiom for this than class
class CSVCommitMessage(val name: String, val csvLocation: String) extends WriterCommitMessage {

}
