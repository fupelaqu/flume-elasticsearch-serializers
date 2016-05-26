package com.ebiznext.flume.sink.elasticsearch.serializer

import java.util

import com.google.common.collect.Maps
import org.apache.commons.lang.StringUtils
import org.apache.flume.Event
import org.apache.flume.event.SimpleEvent
import org.joda.time.DateTimeUtils

/**
  *
  * Created by smanciot on 02/05/16.
  */
class TimestampEvent(base: Event) extends SimpleEvent {
  setBody(base.getBody)
  private[this] val headers: util.HashMap[String, String] = Maps.newHashMap(base.getHeaders)
  private[this] var timestampString = headers.get("timestamp")
  if (StringUtils.isBlank(timestampString)) {
    timestampString = headers.get("@timestamp")
  }
  private[this] val timestamp = if (StringUtils.isBlank(timestampString)) {
    val _timestamp = DateTimeUtils.currentTimeMillis()
    headers.put("timestamp", String.valueOf(_timestamp))
    _timestamp
  } else {
    timestampString.toLong
  }
  setHeaders(headers)

  def getTimestamp = timestamp

}
