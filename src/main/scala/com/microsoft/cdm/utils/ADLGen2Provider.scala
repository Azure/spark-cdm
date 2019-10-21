package com.microsoft.cdm.utils

import org.apache.http.client.methods._
import java.io._
import java.net.URI

import scala.collection.immutable.Stream
import scala.collection.JavaConverters._
import org.apache.commons.httpclient.HttpStatus
import org.apache.commons.io.{Charsets, IOUtils}
import org.apache.http.client.utils.URIBuilder
import org.apache.http.{HttpEntity, HttpResponse, NameValuePair}
import org.apache.http.entity.{FileEntity, StringEntity, ByteArrayEntity}
import org.apache.http.impl.client.{BasicResponseHandler, DefaultHttpClient}
import org.apache.http.message.BasicNameValuePair
import org.apache.commons.io.input.CountingInputStream


import scala.util.Try

/**
  * Provides interface for Azure Data Lake gen2 REST APIs.
  * @param aadProvider Provides AAD tokens.
  */
class ADLGen2Provider(aadProvider: AADProvider) extends Serializable {

  // TODO: should httpclient be a member variable? Would that affect serialization?

  /**
    * Gets the InputStream of a file in ADLSgen2.
    * @param uri ADLSgen2 uri of the file.
    * @return InputStream of the file.
    */
  def getFile(uri: String): InputStream = {
    val request = buildRequest(new HttpGet(), uri)
    execute(request)
  }

  /**
    * Lists files in a directory in ADLSgen2.
    * @param uri ADLSgen2 uri of the directory.
    * @return InputStream of the ADLSgen2 response.
    */
  def listPaths(uri: String): InputStream = {
    val params = Seq(new BasicNameValuePair("recursive", "true"),
      new BasicNameValuePair("resource", "filesystem"))
    val request = buildRequest(new HttpGet(), uri, params)
    execute(request)
  }

  // TODO: clarify overwrite, append, etc.
  /**
    * Upload a string to a ADLSgen2 file.
    * @param data string to upload.
    * @param remotePath ADSgen2 uri to upload to.
    */
  def uploadData(data: String, remotePath: String): Unit = {
    createAndUpload(remotePath, new StringEntity(data, "utf-8"), aadProvider.getToken)
  }

  /**
    * Upload the data in a file to an ADLSgen2 file.
    * @param file file containing data to upload.
    * @param remotePath ADLSgen2 uri to upload to.
    */
  def uploadFile(file: File, remotePath: String): Unit = {
    createAndUpload(remotePath, new FileEntity(file), aadProvider.getToken)
  }

  /**
    * Gets the data in an file returned as a string.
    * @param fileUri URI of the file.
    * @return Data in the file.
    */
  def getFullFile(fileUri: String): String = {
    val inputStream = getFile(fileUri)
    val content = IOUtils.toString(inputStream, Charsets.UTF_8)
    inputStream.close()
    content
  }

  /**
    * Check if a file exists.
    * @param fileUri URI of the file.
    * @return Boolean indicating existence of the file.
    */
  def fileExists(fileUri: String): Boolean = {
    val stream = Try(getFile(fileUri)).getOrElse(null)
    if(stream == null)
      return false
    stream.close()
    true
  }

  /**
    * Get file path for the CSV of a partition. Used to make the filenames for output CSV files.
    * @param modelDirectory Directory of the CDM folder/model.
    * @param entityName Name of the output entity.
    * @param partitionId ID of the partition.
    * @return File path of the output CSV for the partition.
    */
  def getFilePathForPartition(modelDirectory: String, entityName: String, partitionId: Int): String = {
    val fileName = entityName + partitionId.toString
    val directory = modelDirectory + getSep(modelDirectory) + entityName + "/" + entityName + ".csv.snapshots" + "/"
    directory + fileName + ".csv"
  }

  /**
    * Get the file path of the model.json for a CDM folder.
    * @param modelDirectory CDM folder uri.
    * @return The file path of the model.json for a CDM folder.
    */
  def getModelJsonInDirectory(modelDirectory: String): String = {
    "%s%smodel.json".format(modelDirectory, getSep(modelDirectory))
  }

  private def ensureSuccess(response: HttpResponse): Unit = {
    val statusCode = response.getStatusLine.getStatusCode
    if(statusCode != HttpStatus.SC_OK)
    {
      new BasicResponseHandler().handleResponse(response)
    }
  }

  // TODO: proper encoding
  private def encodeURI(uri: String) = {
    uri.replace(" ", "%20")
  }

  private def buildURI(uri: String, params: Seq[NameValuePair]): URI = {
    val uriBuilder = new URIBuilder(encodeURI(uri))
    params.foreach(pair => uriBuilder.addParameter(pair.getName, pair.getValue))
    uriBuilder.build()
  }

  private def buildRequest[T <: HttpRequestBase](request: T,
                                                 uri: String,
                                                 params: Seq[NameValuePair] = Seq()): T = {
    request.setURI(buildURI(uri, params))
    request.addHeader("Authorization", aadProvider.getToken)
    request.addHeader("x-ms-version", "2018-06-17")
    request
  }

  // TODO: ensure we're not leaking InputStreams
  private def execute(request: HttpRequestBase): InputStream = {
    val httpclient = new DefaultHttpClient
    val response = httpclient.execute(request)
    ensureSuccess(response)
    response.getEntity.getContent
  }

  private def appendToFile(uri: String, entity: HttpEntity, bearerToken: String, position: Int): Unit = {
    val params = Seq(new BasicNameValuePair("action", "append"),
      new BasicNameValuePair("position", position.toString))

    val request = buildRequest(new HttpPatch(), uri, params)
    request.setEntity(entity)

    execute(request)
  }

  private def flushFile(uri: String, bearerToken: String, length: Long): Unit = {
    val params = Seq(
      new BasicNameValuePair("action", "flush"),
      new BasicNameValuePair("position", length.toString))

    val request = buildRequest(new HttpPatch(), uri, params)
    execute(request)
  }

  private def createFile(uri: String, bearerToken: String): Unit = {
    val request = buildRequest(new HttpPut(), uri, Seq(
      new BasicNameValuePair("resource", "file")))
    execute(request)
  }

  private def createAndUpload(uri: String, entity: HttpEntity, bearerToken: String): Unit = {
    createFile(uri, bearerToken)
    if(entity.getContentLength > 0) {
      val content = new CountingInputStream(entity.getContent)
      val buffer = new Array[Byte](100000000)

      Stream.continually(content.read(buffer)).takeWhile(_ != -1).foreach(count => {
        val chunk = new ByteArrayEntity(buffer.take(count))
        appendToFile(uri, chunk, bearerToken, content.getCount - count)
      })
      flushFile(uri, bearerToken, content.getCount)
    }
  }

  // TODO: cleaner way to do this
  private def getSep(modelDirectory: String) = {
    if(modelDirectory.charAt(modelDirectory.length - 1) != '/') "/" else ""
  }

}
