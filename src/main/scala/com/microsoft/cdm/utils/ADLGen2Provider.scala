package com.microsoft.cdm.utils

import org.apache.http.client.methods._
import java.io._
import java.net.{URI, URLEncoder}

import scala.collection.JavaConverters._
import org.apache.commons.httpclient.HttpStatus
import org.apache.commons.io.{Charsets, IOUtils}
import org.apache.http.client.utils.URIBuilder
import org.apache.http.{HttpEntity, NameValuePair}
import org.apache.http.entity.{FileEntity, StringEntity}
import org.apache.http.impl.client.{BasicResponseHandler, HttpClients}
import org.apache.http.message.BasicNameValuePair

import scala.util.Try

class ADLGen2Provider(aadProvider: AADProvider) extends Serializable {

  // TODO: should httpclient be a member variable? Would that affect serialization?

  private def ensureSuccess(response: CloseableHttpResponse): Unit = {
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
    uriBuilder.addParameters(params.asJava)
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
    val response = HttpClients.createDefault.execute(request)
    ensureSuccess(response)
    response.getEntity.getContent
  }

  private def appendToFile(uri: String, entity: HttpEntity, bearerToken: String): Unit = {
    val params = Seq(new BasicNameValuePair("action", "append"),
      new BasicNameValuePair("position", "0"))

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
      appendToFile(uri, entity, bearerToken)
      flushFile(uri, bearerToken, entity.getContentLength)
    }
  }

  def getFile(uri: String): InputStream = {
    val request = buildRequest(new HttpGet(), uri)
    execute(request)
  }

  def listPaths(uri: String): InputStream = {
    val params = Seq(new BasicNameValuePair("recursive", "true"),
      new BasicNameValuePair("resource", "filesystem"))
    val request = buildRequest(new HttpGet(), uri, params)
    execute(request)
  }

  def uploadData(data: String, remotePath: String): Unit = {
    createAndUpload(remotePath, new StringEntity(data, "utf-8"), aadProvider.getToken)
  }

  def uploadFile(file: File, remotePath: String): Unit = {
    createAndUpload(remotePath, new FileEntity(file), aadProvider.getToken)
  }

  def getFullFile(fileUri: String): String = {
    val inputStream = getFile(fileUri)
    val content = IOUtils.toString(inputStream, Charsets.UTF_8)
    inputStream.close()
    content
  }

  // TODO: probably a cleaner API for this
  def fileExists(fileUri: String): Boolean = {
    val stream = Try(getFile(fileUri)).getOrElse(null)
    if(stream == null)
      return false
    stream.close()
    true
  }

  // TODO: cleaner way to do this
  private def getSep(modelDirectory: String) = {
    if(modelDirectory.charAt(modelDirectory.length - 1) != '/') "/" else ""
  }

  def getFilePathForPartition(modelDirectory: String, entityName: String, partitionId: Int): String = {
    val fileName = entityName + partitionId.toString
    val directory = modelDirectory + getSep(modelDirectory) + entityName + "/" + entityName + ".csv.snapshots" + "/"
    directory + fileName + ".csv"
  }

  def getModelJsonInDirectory(modelDirectory: String): String = {
    "%s%smodel.json".format(modelDirectory, getSep(modelDirectory))
  }

}
