package com.ebiznext.flume.elasticsearch.serializer

import java.util.TimeZone

import org.apache.commons.lang.time.FastDateFormat
import org.apache.flume.Context
import org.apache.flume.Event
import org.apache.flume.conf.ComponentConfiguration
import org.apache.flume.formatter.output.BucketPath
import org.apache.flume.sink.elasticsearch.{ElasticSearchDynamicSerializer, ElasticSearchIndexRequestBuilderFactory}
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest
import org.elasticsearch.action.index.IndexRequestBuilder
import org.elasticsearch.client.Client

import com.google.common.annotations.VisibleForTesting
import org.elasticsearch.indices.IndexAlreadyExistsException

import scala.io.Source
import scala.util.Success

/**
  *
  * Created by smanciot on 02/05/16.
  */
class ElasticSearchEventSerializerWithTypeMappings extends ElasticSearchIndexRequestBuilderFactory{

  import ElasticSearchEventSerializerWithTypeMappings._

  type IndexPrefix = String

  private[this] var indicesMappings = Map[IndexPrefix, String]()

  private[this] var mappingsCache = Set[String]()

  private[this] val fastDateFormat: FastDateFormat = FastDateFormat.getInstance("yyyy-MM-dd", TimeZone.getTimeZone("Etc/UTC"))

  private[this] val serializer = new ElasticSearchDynamicSerializer

  private def mappings(source: Source) = using(source){
    source => source.mkString
  }

  override def createIndexRequest(client: Client, indexPrefix: String, indexType: String, event: Event): IndexRequestBuilder = {
    val request = prepareIndex(client)
    val realIndexPrefix = BucketPath.escapeString(indexPrefix, event.getHeaders)
    val realIndexType = BucketPath.escapeString(indexType, event.getHeaders)
    val timestampedEvent = new TimestampEvent(event)
    val timestamp = timestampedEvent.getTimestamp
    val indexName = getIndexName(realIndexPrefix, timestamp)
    if (!mappingsCache.contains(s"$indexName.$indexType") && indicesMappings.contains(s"$indexPrefix.$indexType")){
      createIndexWithMapping(client, indexName, realIndexType, indicesMappings.get(s"$indexPrefix.$indexType").get)
    }
    prepareIndexRequest(request, indexName, realIndexType, timestampedEvent)
    request
  }

  override def configure(conf: ComponentConfiguration): Unit = {}

  override def configure(context: Context): Unit = {
    Option(context.getString(CONF_INDICES)) match {
      case Some(s) => s.split(",").foreach{
        indice => context.getSubProperties(s"$indice.").get(CONF_TYPES) match {
          case types: String => types.split(",").foreach{
            t => context.getSubProperties(s"$indice.$t.").get(CONF_MAPPINGS_FILE) match {
              case file: String => mappings(Source.fromFile(file)) match {
                case Success(mappings) =>
                  indicesMappings = indicesMappings + (s"$indice.$t" -> mappings)
                case _ =>
              }
              case _ =>
            }
          }
          case _ =>
        }
      }
      case _ =>
    }
  }

  @VisibleForTesting
  def prepareIndex(client: Client): IndexRequestBuilder = client.prepareIndex()

  /**
    * Gets the name of the index to use for an index request
    *
    * @return index name of the form 'indexPrefix-formattedTimestamp'
    * @param indexPrefix
    *          Prefix of index name to use -- as configured on the sink
    * @param timestamp
    *          timestamp (millis) to format / use
    */
  def getIndexName(indexPrefix: String, timestamp: Long): String =
    new StringBuilder(indexPrefix).append('-')
      .append(fastDateFormat.format(timestamp)).toString()

  /**
    * Prepares an ElasticSearch IndexRequestBuilder instance
    *
    * @param indexRequest
    *          The (empty) ElasticSearch IndexRequestBuilder to prepare
    * @param indexName
    *          Index name to use -- as per #getIndexName(String, long)
    * @param indexType
    *          Index type to use -- as configured on the sink
    * @param event
    *          Flume event to serialize and add to index request
    */
  protected def prepareIndexRequest(indexRequest: IndexRequestBuilder, indexName: String, indexType: String, event: Event) {
    val contentBuilder = serializer.getContentBuilder(event)
    indexRequest.setIndex(indexName)
      .setType(indexType)
      .setSource(contentBuilder.bytes())
  }

  /**
    * Creates the index type mappings defined by the user
    *
    * @param client
    *       	  ElasticSearch Client
    * @param indexName
    *            Index name to use -- as per #getIndexName(String, long)
    * @param indexType
    *            Index type to use -- as configured on the sink
    * @param jsonMappings
    *            Index type json mappings to use
    */
  private def createIndexWithMapping(client: Client, indexName: String, indexType: String, jsonMappings: String) {
    try {
      client.admin().indices().prepareCreate(indexName).addMapping(indexType, jsonMappings).get()
      mappingsCache = mappingsCache + s"$indexName.$indexType"
    } catch {
      case e:IndexAlreadyExistsException => throw e
    }
  }

  def getIndicesMappings = Map[String, String]() ++ indicesMappings

  def getMappingsCache = Set[String]() ++ mappingsCache
}

object ElasticSearchEventSerializerWithTypeMappings{
  val CONF_INDICES = "indices"

  val CONF_TYPES = "types"

  val CONF_MAPPINGS_FILE = "mappingsFile"

}