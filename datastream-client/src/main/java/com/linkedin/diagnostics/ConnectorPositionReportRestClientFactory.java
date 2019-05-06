/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.diagnostics;

import java.util.HashMap;
import java.util.Map;

import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.datastream.BaseRestClientFactory;
import com.linkedin.datastream.DatastreamRestClientFactory;
import com.linkedin.r2.transport.http.client.HttpClientFactory;
import com.linkedin.restli.client.RestClient;

/**
 * Factory class for obtaining a {@link ConnectorPositionReportRestClient}.
 */
public final class ConnectorPositionReportRestClientFactory {

  private static final Logger LOG = LoggerFactory.getLogger(DatastreamRestClientFactory.class);
  private static final BaseRestClientFactory<ConnectorPositionReportRestClient> FACTORY =
      new BaseRestClientFactory<>(ConnectorPositionReportRestClient.class, LOG);

  /**
   * Gets a {@link ConnectorPositionReportRestClient} with default options.
   *
   * @param dmsUri The URI for the DMS endpoint
   * @return a {@link ConnectorPositionReportRestClient}
   */
  public static ConnectorPositionReportRestClient getClient(@NotNull final String dmsUri) {
    return getClient(dmsUri, new HashMap<>());
  }

  /**
   * Gets a {@link ConnectorPositionReportRestClient} with custom options for the Rest.li client used for sending
   * requests.
   *
   * @param dmsUri The URI for the DMS endpoint
   * @param httpClientProperties Configuration properties that will be provided when creating the Rest.li client used by
   *                             {@link ConnectorPositionReportRestClient}.
   */
  public static ConnectorPositionReportRestClient getClient(@NotNull final String dmsUri,
      @NotNull final Map<String, Object> httpClientProperties) {
    // A ConnectorPositionReportRestClient response can be very large if Brooklin is responsible for lots of partitions.
    // These defaults should accommodate almost all use cases.
    httpClientProperties.putIfAbsent(HttpClientFactory.HTTP_MAX_RESPONSE_SIZE, "256000000"); // 256 MB
    httpClientProperties.putIfAbsent(HttpClientFactory.HTTP_REQUEST_TIMEOUT, "10000"); // 10 seconds
    return FACTORY.getClient(dmsUri, httpClientProperties);
  }

  /**
   * @see BaseRestClientFactory#registerRestClient(String, RestClient)
   */
  public static void registerRestClient(@NotNull final String dmsUri, @NotNull final RestClient restClient) {
    FACTORY.registerRestClient(dmsUri, restClient);
  }
}