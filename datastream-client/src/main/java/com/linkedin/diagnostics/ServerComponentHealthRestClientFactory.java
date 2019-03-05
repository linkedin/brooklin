/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.diagnostics;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.datastream.BaseRestClientFactory;
import com.linkedin.datastream.DatastreamRestClientFactory;
import com.linkedin.r2.transport.http.client.HttpClientFactory;
import com.linkedin.restli.client.RestClient;


/**
 * Factory class for obtaining {@link ServerComponentHealthRestClient} objects.
 */
public final class ServerComponentHealthRestClientFactory {
  private static final Logger LOG = LoggerFactory.getLogger(DatastreamRestClientFactory.class);
  private static final BaseRestClientFactory<ServerComponentHealthRestClient> FACTORY =
      new BaseRestClientFactory<>(ServerComponentHealthRestClient.class, LOG);

  /**
   * Get a ServerComponentHealthRestClient with default HTTP client
   * @param dmsUri URI to DMS endpoint
   */
  public static ServerComponentHealthRestClient getClient(String dmsUri) {
    // A ServerComponentHealth response can be very large if Brooklin is responsible for lots of partitions.
    // These defaults should accommodate almost all use cases.
    final Map<String, String> properties = new HashMap<>();
    properties.put(HttpClientFactory.HTTP_MAX_RESPONSE_SIZE, "256000000"); // 256 MB
    properties.put(HttpClientFactory.HTTP_REQUEST_TIMEOUT, "10000"); // 10 seconds

    return FACTORY.getClient(dmsUri, properties);
  }

  /**
   * Get a ServerComponentHealthRestClient with custom HTTP configs
   * @see BaseRestClientFactory#getClient(String, Map)
   * @param dmsUri URI to DMS endpoint
   * @param httpConfig custom config for HTTP client, please find the configs in
   *                   {@link com.linkedin.r2.transport.http.client.HttpClientFactory}
   */
  public static ServerComponentHealthRestClient getClient(String dmsUri, Map<String, String> httpConfig) {
    return FACTORY.getClient(dmsUri, httpConfig);
  }

  /**
   * @see BaseRestClientFactory#registerRestClient(String, RestClient)
   */
  public static void registerRestClient(String dmsUri, RestClient restClient) {
    FACTORY.registerRestClient(dmsUri, restClient);
  }
}
