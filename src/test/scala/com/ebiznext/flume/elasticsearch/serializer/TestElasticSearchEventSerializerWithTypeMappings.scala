package com.ebiznext.flume.elasticsearch.serializer

import com.ebiznext.flume.elasticsearch.EmbeddedElasticSearchNode
import org.apache.flume.event.EventBuilder
import org.elasticsearch.node.Node
import org.junit.{Test, After, Before}

import org.junit.Assert._

import org.apache.flume._

import com.ebiznext.flume.elasticsearch.serializer.ElasticSearchEventSerializerWithTypeMappings._

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
    val indexName = serializer.getIndexName("i1", event.getTimestamp)
    val client = esNode.client()
    serializer.createIndexRequest(client, "i1", "t1", event)
    assertEquals(1, serializer.getMappingsCache.size)
    val mapping = client.admin().indices().prepareGetMappings(indexName).get().getMappings.get(indexName).get("t1")
    assertTrue(Option(mapping).isDefined)
  }

}
