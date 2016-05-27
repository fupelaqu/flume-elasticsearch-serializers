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
package com.ebiznext.flume.sink.elasticsearch.client;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang.StringUtils;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.sink.elasticsearch.ElasticSearchIndexRequestBuilderFactory;
import org.apache.flume.sink.elasticsearch.IndexNameBuilder;
import org.apache.flume.sink.elasticsearch.client.ElasticSearchClient;
import org.apache.flume.sink.elasticsearch.client.ElasticSearchTransportClient;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

import static org.apache.flume.sink.elasticsearch.ElasticSearchSinkConstants.DEFAULT_PORT;

import static com.ebiznext.flume.sink.elasticsearch.client.SearchGuardElasticSearchTransportClientConstants.*;

public class SearchGuardElasticSearchTransportClient implements ElasticSearchClient {

  public static final Logger logger = LoggerFactory
      .getLogger(ElasticSearchTransportClient.class);

  private InetSocketTransportAddress[] serverAddresses;
  private ElasticSearchIndexRequestBuilderFactory indexRequestBuilderFactory;
  private BulkRequestBuilder bulkRequestBuilder;

  private Client client;

  private String credentials;

  public SearchGuardElasticSearchTransportClient(String[] hostNames, String clusterName,
                                                 ElasticSearchIndexRequestBuilderFactory indexBuilder) {
    configureHostnames(hostNames);
    this.indexRequestBuilderFactory = indexBuilder;
    openClient(clusterName);
  }

  private void configureHostnames(String[] hostNames) {
    logger.warn(Arrays.toString(hostNames));
    serverAddresses = new InetSocketTransportAddress[hostNames.length];
    for (int i = 0; i < hostNames.length; i++) {
      String[] hostPort = hostNames[i].trim().split(":");
      String host = hostPort[0].trim();
      int port = hostPort.length == 2 ? Integer.parseInt(hostPort[1].trim())
              : DEFAULT_PORT;
      serverAddresses[i] = new InetSocketTransportAddress(host, port);
    }
  }
  
  @Override
  public void close() {
    if (client != null) {
      client.close();
    }
    client = null;
  }

  @Override
  public void addEvent(Event event, IndexNameBuilder indexNameBuilder,
      String indexType, long ttlMs) throws Exception {
    if (bulkRequestBuilder == null) {
      bulkRequestBuilder = client.prepareBulk();
      if(StringUtils.isNotEmpty(credentials)){
        bulkRequestBuilder.putHeader("searchguard_transport_creds", credentials);
      }
    }

    IndexRequestBuilder indexRequestBuilder = null;
    indexRequestBuilder = indexRequestBuilderFactory.createIndexRequest(
            client, indexNameBuilder.getIndexPrefix(event), indexType, event);
    if (ttlMs > 0) {
      indexRequestBuilder.setTTL(ttlMs);
    }
    bulkRequestBuilder.add(indexRequestBuilder);
  }

  @Override
  public void execute() throws Exception {
    try {
      BulkResponse bulkResponse = bulkRequestBuilder.execute().actionGet();
      if (bulkResponse.hasFailures()) {
        throw new EventDeliveryException(bulkResponse.buildFailureMessage());
      }
    } finally {
      bulkRequestBuilder = client.prepareBulk();
      if(StringUtils.isNotEmpty(credentials)){
        bulkRequestBuilder.putHeader("searchguard_transport_creds", credentials);
      }
    }
  }

  /**
   * Open client to elaticsearch cluster
   * 
   * @param clusterName
   */
  private void openClient(String clusterName) {
    logger.info("Using ElasticSearch hostnames: {} ",
        Arrays.toString(serverAddresses));
    Settings settings = ImmutableSettings.settingsBuilder()
        .put("cluster.name", clusterName).build();

    TransportClient transportClient = new TransportClient(settings);
    for (InetSocketTransportAddress host : serverAddresses) {
      transportClient.addTransportAddress(host);
    }
    if (client != null) {
      client.close();
    }
    client = transportClient;
  }

  @Override
  public void configure(Context context) {
    String username = context.getString(SEARCH_GUARD_USERNAME);
    String password = context.getString(SEARCH_GUARD_PASSWORD);
    if(StringUtils.isNotEmpty(username) && StringUtils.isNotEmpty(password)){
      credentials = Base64.encodeBase64String(username.concat(":").concat(password).getBytes());
    }
    else{
      logger.warn("no credentials have been specified");
    }
  }
}
