/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.ebiznext.flume.elasticsearch;

import com.ebiznext.flume.elasticsearch.client.SearchGuardElasticSearchTransportClient;
import com.ebiznext.flume.elasticsearch.serializer.ElasticSearchEventSerializerWithTypeMappings;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import org.apache.commons.lang.StringUtils;
import org.apache.flume.*;
import org.apache.flume.Sink.Status;
import org.apache.flume.conf.Configurable;
import org.apache.flume.formatter.output.BucketPath;
import org.apache.flume.instrumentation.SinkCounter;
import org.apache.flume.sink.AbstractSink;
import org.apache.flume.sink.elasticsearch.*;
import org.apache.flume.sink.elasticsearch.client.ElasticSearchClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.flume.sink.elasticsearch.ElasticSearchSinkConstants.*;

/**
 * A sink which reads events from a channel and writes them to ElasticSearch
 * based on the work done by https://github.com/Aconex/elasticflume.git.</p>
 * 
 * This sink supports batch reading of events from the channel and writing them
 * to ElasticSearch.</p>
 * 
 * Indexes will be rolled daily using the format 'indexname-YYYY-MM-dd' to allow
 * easier management of the index</p>
 * 
 * This sink must be configured with mandatory parameters detailed in
 * {@link ElasticSearchSinkConstants}</p> It is recommended as a secondary step
 * the ElasticSearch indexes are optimized for the specified serializer. This is
 * not handled by the sink but is typically done by deploying a config template
 * alongside the ElasticSearch deploy</p>
 * 
 */
public class SearchGuardElasticSearchSink  extends AbstractSink implements Configurable {

  private static final Logger logger = LoggerFactory.getLogger(SearchGuardElasticSearchSink.class);

  private final CounterGroup counterGroup = new CounterGroup();

  private static final int defaultBatchSize = 100;

  private int batchSize = defaultBatchSize;
  private long ttlMs = DEFAULT_TTL;
  private String clusterName = DEFAULT_CLUSTER_NAME;
  private String indexName = DEFAULT_INDEX_NAME;
  private String indexType = DEFAULT_INDEX_TYPE;
  private final Pattern pattern = Pattern.compile(TTL_REGEX, Pattern.CASE_INSENSITIVE);
  private Matcher matcher = pattern.matcher("");

  private String[] serverAddresses = null;

  private ElasticSearchClient client = null;
  private Context elasticSearchClientContext = null;

  private ElasticSearchIndexRequestBuilderFactory indexRequestFactory;
  private IndexNameBuilder indexNameBuilder;
  private SinkCounter sinkCounter;

  /**
   * Create an {@link org.apache.flume.sink.elasticsearch.ElasticSearchSink} configured using the supplied
   * configuration
   */
  public SearchGuardElasticSearchSink() {

  }

  @Override
  public Status process() throws EventDeliveryException {
    logger.debug("processing...");
    Status status = Status.READY;
    Channel channel = getChannel();
    Transaction txn = channel.getTransaction();
    try {
      txn.begin();
      int count;
      for (count = 0; count < batchSize; ++count) {
        Event event = channel.take();

        if (event == null) {
          break;
        }
        String realIndexType = BucketPath.escapeString(indexType, event.getHeaders());
        client.addEvent(event, indexNameBuilder, realIndexType, ttlMs);
      }

      if (count <= 0) {
        sinkCounter.incrementBatchEmptyCount();
        counterGroup.incrementAndGet("channel.underflow");
        status = Status.BACKOFF;
      } else {
        if (count < batchSize) {
          sinkCounter.incrementBatchUnderflowCount();
          status = Status.BACKOFF;
        } else {
          sinkCounter.incrementBatchCompleteCount();
        }

        sinkCounter.addToEventDrainAttemptCount(count);
        client.execute();
      }
      txn.commit();
      sinkCounter.addToEventDrainSuccessCount(count);
      counterGroup.incrementAndGet("transaction.success");
    } catch (Throwable ex) {
      try {
        txn.rollback();
        counterGroup.incrementAndGet("transaction.rollback");
      } catch (Exception ex2) {
        logger.error(
            "Exception in rollback. Rollback might not have been successful.",
            ex2);
      }

      if (ex instanceof Error || ex instanceof RuntimeException) {
        logger.error("Failed to commit transaction. Transaction rolled back.",
            ex);
        Throwables.propagate(ex);
      } else {
        logger.error("Failed to commit transaction. Transaction rolled back.",
            ex);
        throw new EventDeliveryException(
            "Failed to commit transaction. Transaction rolled back.", ex);
      }
    } finally {
      txn.close();
    }
    return status;
  }

  @Override
  public void configure(Context context) {
    if (StringUtils.isNotBlank(context.getString(HOSTNAMES))) {
      serverAddresses = StringUtils.deleteWhitespace(
          context.getString(HOSTNAMES)).split(",");
    }
    Preconditions.checkState(serverAddresses != null
        && serverAddresses.length > 0, "Missing Param:" + HOSTNAMES);

    if (StringUtils.isNotBlank(context.getString(INDEX_NAME))) {
      this.indexName = context.getString(INDEX_NAME);
    }

    if (StringUtils.isNotBlank(context.getString(INDEX_TYPE))) {
      this.indexType = context.getString(INDEX_TYPE);
    }

    if (StringUtils.isNotBlank(context.getString(CLUSTER_NAME))) {
      this.clusterName = context.getString(CLUSTER_NAME);
    }

    if (StringUtils.isNotBlank(context.getString(BATCH_SIZE))) {
      this.batchSize = Integer.parseInt(context.getString(BATCH_SIZE));
    }

    if (StringUtils.isNotBlank(context.getString(TTL))) {
      this.ttlMs = parseTTL(context.getString(TTL));
      Preconditions.checkState(ttlMs > 0, TTL
          + " must be greater than 0 or not set.");
    }

    elasticSearchClientContext = new Context();
    elasticSearchClientContext.putAll(context.getSubProperties(CLIENT_PREFIX));

    Context serializerContext = new Context();
    serializerContext.putAll(context.getSubProperties(SERIALIZER_PREFIX));
    serializerContext.putAll(context.getSubProperties(CLIENT_PREFIX));

    indexRequestFactory = new ElasticSearchEventSerializerWithTypeMappings();
    indexRequestFactory.configure(serializerContext);

    if (sinkCounter == null) {
      sinkCounter = new SinkCounter(getName());
    }

    String indexNameBuilderClass = DEFAULT_INDEX_NAME_BUILDER_CLASS;
    if (StringUtils.isNotBlank(context.getString(INDEX_NAME_BUILDER))) {
      indexNameBuilderClass = context.getString(INDEX_NAME_BUILDER);
    }

    Context indexnameBuilderContext = new Context();
    serializerContext.putAll(
            context.getSubProperties(INDEX_NAME_BUILDER_PREFIX));

    try {
      @SuppressWarnings("unchecked")
      Class<? extends IndexNameBuilder> clazz
              = (Class<? extends IndexNameBuilder>) Class
              .forName(indexNameBuilderClass);
      indexNameBuilder = clazz.newInstance();
      indexnameBuilderContext.put(INDEX_NAME, indexName);
      indexNameBuilder.configure(indexnameBuilderContext);
    } catch (Exception e) {
      logger.error("Could not instantiate index name builder.", e);
      Throwables.propagate(e);
    }

    if (sinkCounter == null) {
      sinkCounter = new SinkCounter(getName());
    }

    Preconditions.checkState(StringUtils.isNotBlank(indexName),
        "Missing Param:" + INDEX_NAME);
    Preconditions.checkState(StringUtils.isNotBlank(indexType),
        "Missing Param:" + INDEX_TYPE);
    Preconditions.checkState(StringUtils.isNotBlank(clusterName),
        "Missing Param:" + CLUSTER_NAME);
    Preconditions.checkState(batchSize >= 1, BATCH_SIZE
        + " must be greater than 0");
  }

  @Override
  public void start() {
    logger.info("ElasticSearch sink {} started");
    sinkCounter.start();
    try {
      client = new SearchGuardElasticSearchTransportClient(serverAddresses,
          clusterName, indexRequestFactory);
      client.configure(elasticSearchClientContext);
      sinkCounter.incrementConnectionCreatedCount();
    } catch (Exception ex) {
      ex.printStackTrace();
      sinkCounter.incrementConnectionFailedCount();
      if (client != null) {
        client.close();
        sinkCounter.incrementConnectionClosedCount();
      }
    }

    super.start();
  }

  @Override
  public void stop() {
    logger.info("ElasticSearch sink {} stopping");
    if (client != null) {
      client.close();
    }
    sinkCounter.incrementConnectionClosedCount();
    sinkCounter.stop();
    super.stop();
  }

  /*
   * Returns TTL value of ElasticSearch index in milliseconds when TTL specifier
   * is "ms" / "s" / "m" / "h" / "d" / "w". In case of unknown specifier TTL is
   * not set. When specifier is not provided it defaults to days in milliseconds
   * where the number of days is parsed integer from TTL string provided by
   * user. <p> Elasticsearch supports ttl values being provided in the format:
   * 1d / 1w / 1ms / 1s / 1h / 1m specify a time unit like d (days), m
   * (minutes), h (hours), ms (milliseconds) or w (weeks), milliseconds is used
   * as default unit.
   * http://www.elasticsearch.org/guide/reference/mapping/ttl-field/.
   * 
   * @param ttl TTL value provided by user in flume configuration file for the
   * sink
   * 
   * @return the ttl value in milliseconds
   */
  private long parseTTL(String ttl) {
    matcher = matcher.reset(ttl);
    while (matcher.find()) {
      if (matcher.group(2).equals("ms")) {
        return Long.parseLong(matcher.group(1));
      } else if (matcher.group(2).equals("s")) {
        return TimeUnit.SECONDS.toMillis(Integer.parseInt(matcher.group(1)));
      } else if (matcher.group(2).equals("m")) {
        return TimeUnit.MINUTES.toMillis(Integer.parseInt(matcher.group(1)));
      } else if (matcher.group(2).equals("h")) {
        return TimeUnit.HOURS.toMillis(Integer.parseInt(matcher.group(1)));
      } else if (matcher.group(2).equals("d")) {
        return TimeUnit.DAYS.toMillis(Integer.parseInt(matcher.group(1)));
      } else if (matcher.group(2).equals("w")) {
        return TimeUnit.DAYS.toMillis(7 * Integer.parseInt(matcher.group(1)));
      } else if (matcher.group(2).equals("")) {
        logger.info("TTL qualifier is empty. Defaulting to day qualifier.");
        return TimeUnit.DAYS.toMillis(Integer.parseInt(matcher.group(1)));
      } else {
        logger.debug("Unknown TTL qualifier provided. Setting TTL to 0.");
        return 0;
      }
    }
    logger.info("TTL not provided. Skipping the TTL config by returning 0.");
    return 0;
  }
}
