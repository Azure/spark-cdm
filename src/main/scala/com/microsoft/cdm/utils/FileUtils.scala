package com.microsoft.cdm.utils

import java.io.File
import org.apache.commons.io.FilenameUtils

object FileUtils {

  def deleteFile(filePath: String): Boolean = {
    new File(filePath).delete()
  }

  def getTempFile(prefix: String): File = {
    File.createTempFile("private", "cache")
  }

  def basename(path: String): String = {
    FilenameUtils.getBaseName(path)
  }

}
