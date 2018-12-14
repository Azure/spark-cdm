package com.microsoft.cdm.utils

import java.io.OutputStreamWriter

import com.univocity.parsers.csv.{CsvParser, CsvParserSettings, CsvWriter, CsvWriterSettings}

/**
  * Builds Univocity CsvParser instances.
  */
object CsvParserFactory {

  def build(): CsvParser = {
    val settings = new CsvParserSettings()
    val format = settings.getFormat
    format.setDelimiter(',')
    settings.setMaxCharsPerColumn(500000)
    new CsvParser(settings)
  }

  def buildWriter(outputWriter: OutputStreamWriter): CsvWriter = {
    new CsvWriter(outputWriter, new CsvWriterSettings())
  }

}
