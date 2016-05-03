package com.ebiznext.flume.elasticsearch.serializer

import com.ebiznext.flume.elasticsearch.EmbeddedElasticSearchNode
import org.apache.flume.event.EventBuilder
import org.elasticsearch.node.Node
import org.specs2.mutable.Specification

import org.apache.flume._

import com.ebiznext.flume.elasticsearch.serializer.ElasticSearchEventSerializerWithTypeMappings._
import org.specs2.specification.BeforeAfterExample

/**
  *
  * Created by smanciot on 03/05/16.
  */
class ElasticSearchEventSerializerWithTypeMappingsSpec extends Specification  with EmbeddedElasticSearchNode with BeforeAfterExample{

  sequential

  // Node ElasticSearch utilisé pour chaque test.
  var esNode : Node = null

  override def before = {
    esNode = startEmbedded()
  }

  override def after = {
    prepareRefresh(esNode)
  }

  "configuration" should {
    "add index type mappings" in {
      val context = new Context
      val serializer = new ElasticSearchEventSerializerWithTypeMappings
      serializer.configure(context)
      serializer.getIndicesMappings.isEmpty must_==true
      context.put(CONF_INDICES, "i1,i2")
      context.put(s"i1.$CONF_TYPES", "t1,t2")
      context.put(s"i1.t1.$CONF_MAPPINGS_FILE", getClass.getResource("/t1.json").getPath)
      serializer.configure(context)
      serializer.getIndicesMappings.size must_==1
      context.put(s"i1.t2.$CONF_MAPPINGS_FILE", getClass.getResource("/t1.json").getPath)
      serializer.configure(context)
      serializer.getIndicesMappings.size must_==2
    }
  }

  "timestamp event" should {
    "add index type mappings" in {
      val context = new Context
      val serializer = new ElasticSearchEventSerializerWithTypeMappings
      context.put(CONF_INDICES, "i1")
      context.put(s"i1.$CONF_TYPES", "t1")
      context.put(s"i1.t1.$CONF_MAPPINGS_FILE", getClass.getResource("/t1.json").getPath)
      serializer.configure(context)
      serializer.getIndicesMappings.size must_==1
      val event = new TimestampEvent(EventBuilder.withBody("{\"p1\": 1}".getBytes()))
      val indexName = serializer.getIndexName("i1", event.getTimestamp)
      val client = esNode.client()
      serializer.createIndexRequest(client, "i1", "t1", event)
      serializer.getMappingsCache.size must_==1
      val mapping = client.admin().indices().prepareGetMappings(indexName).get().getMappings.get(indexName).get("t1")
      Option(mapping).isDefined must_==true
    }
  }
}
