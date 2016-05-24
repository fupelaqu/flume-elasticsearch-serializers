package com.ebiznext.flume.elasticsearch.serializer

import java.util.UUID

import com.ebiznext.flume.elasticsearch.{SearchGuardElasticSearchSink, EmbeddedElasticSearchNode}
import com.ebiznext.flume.elasticsearch.conf.Settings
import org.apache.flume.channel.MemoryChannel
import org.apache.flume.conf.Configurables
import org.apache.flume.event.EventBuilder
import org.apache.flume.sink.elasticsearch.ElasticSearchSink
import org.apache.flume.sink.elasticsearch.ElasticSearchSinkConstants._
import org.elasticsearch.client.Requests
import org.elasticsearch.cluster.metadata.MappingMetaData
import org.elasticsearch.index.query.QueryBuilders
import org.elasticsearch.node.Node
import org.junit.{Test, After, Before}

import org.junit.Assert._

import org.apache.flume._

import com.ebiznext.flume.elasticsearch.serializer.ElasticSearchEventSerializerWithTypeMappings._
import com.ebiznext.flume.elasticsearch.client.SearchGuardElasticSearchTransportClientConstants._

/**
  *
  * Created by smanciot on 03/05/16.
  */
class TestElasticSearchEventSerializerWithTypeMappings extends EmbeddedElasticSearchNode {

  // Node ElasticSearch utilisé pour chaque test.
  var esNode : Node = null

  @Before
  def setUp(): Unit = {
    esNode = startEmbedded()
  }

  @After
  def tearDown(): Unit = {
    stopEmbedded(esNode)
  }

  @Test
  def testConfiguration() = {
    val context = new Context
    val serializer = new ElasticSearchEventSerializerWithTypeMappings
    serializer.configure(context)
    assertTrue(serializer.getIndicesMappings.isEmpty)
    context.put(CONF_INDICES, "i1,i2")
    context.put(s"i1.$CONF_TYPES", "t1,t2")
    context.put(s"i1.t1.$CONF_MAPPINGS_FILE", getClass.getResource("/t1.json").getPath)
    serializer.configure(context)
    assertEquals(1, serializer.getIndicesMappings.size)
    context.put(s"i1.t2.$CONF_MAPPINGS_FILE", getClass.getResource("/t1.json").getPath)
    serializer.configure(context)
    assertEquals(2, serializer.getIndicesMappings.size)
  }

  @Test
  def testTimestampEvent() = {
    val context = new Context
    val serializer = new ElasticSearchEventSerializerWithTypeMappings
    context.put(CONF_INDICES, "i1")
    context.put(s"i1.$CONF_TYPES", "t1")
    context.put(s"i1.t1.$CONF_MAPPINGS_FILE", getClass.getResource("/t1.json").getPath)
    serializer.configure(context)
    assertEquals(1, serializer.getIndicesMappings.size)
    val event = new TimestampEvent(EventBuilder.withBody("{\"p1\": 1}".getBytes()))
    val indexName = ElasticSearchEventSerializerWithTypeMappings.getIndexName("i1", event.getTimestamp)
    val client = esNode.client()
    serializer.createIndexRequest(client, "i1", "t1", event)
    assertEquals(1, serializer.getMappingsCache.size)
    val mapping = client.admin().indices().prepareGetMappings(indexName).get().getMappings.get(indexName).get("t1")
    assertTrue(Option(mapping).isDefined)
  }

  @Test
  def testElasticSearchSink() = {
    val fixture = new SearchGuardElasticSearchSink
    fixture.setName("SearchGuardElasticSearchSink-" + UUID.randomUUID.toString)

    // Configure ElasticSearch Sink
    val context = new Context
    context.put(HOSTNAMES, "localhost:9300")
    context.put(INDEX_NAME, "%{index}")
    context.put(INDEX_TYPE, "%{type}")
    context.put(CLUSTER_NAME, Settings.ElasticSearch.Cluster)
    context.put(BATCH_SIZE, "1")
    val classz = ElasticSearchEventSerializerWithTypeMappings.getClass
    context.put(SERIALIZER, "com.ebiznext.flume.elasticsearch.serializer.ElasticSearchEventSerializerWithTypeMappings")
    context.put(SERIALIZER_PREFIX+CONF_INDICES, "i1")
    context.put(s"${SERIALIZER_PREFIX}i1.$CONF_TYPES", "t1")
    context.put(s"${SERIALIZER_PREFIX}i1.t1.$CONF_MAPPINGS_FILE", getClass.getResource("/t1.json").getPath)
    context.put(CLIENT_PREFIX+SEARCH_GUARD_USERNAME, "root")
    context.put(CLIENT_PREFIX+SEARCH_GUARD_PASSWORD, "changeit")
    Configurables.configure(fixture, context)

    // Configure the channel
    val channel: Channel = new MemoryChannel
    Configurables.configure(channel, new Context)

    // Wire them together
    fixture.setChannel(channel)
    fixture.start()

    val tx: Transaction = channel.getTransaction

    tx.begin()

    import scala.collection.JavaConversions._

    val headers = Map[String, String]("index" -> "i1", "type" -> "t1")
    val e = EventBuilder.withBody("{\"p1\": 1}".getBytes())
    e.setHeaders(headers)
    val event: TimestampEvent = new TimestampEvent(e)
    channel.put(event)

    tx.commit()
    tx.close()

    fixture.process
    fixture.stop()

    val indexName = ElasticSearchEventSerializerWithTypeMappings.getIndexName("i1", event.getTimestamp)
    val client = esNode.client()
    val mapping: MappingMetaData = client.admin().indices().prepareGetMappings(indexName).get().getMappings.get(indexName).get("t1")
    assertTrue(Option(mapping).isDefined)

    client.admin.indices().refresh(Requests.refreshRequest(indexName)).actionGet()
    val response = client.prepareSearch(indexName).setTypes("t1").setQuery(QueryBuilders.matchAllQuery).execute().actionGet()
    val hits = response.getHits
    assertEquals(1, hits.getTotalHits)
    assertEquals(new String(event.getBody), hits.getAt(0).getSourceAsString)
  }
}
