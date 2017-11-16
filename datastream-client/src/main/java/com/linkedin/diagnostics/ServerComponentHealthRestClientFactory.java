package com.linkedin.diagnostics;

import java.util.Collections;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.datastream.BaseRestClientFactory;
import com.linkedin.datastream.DatastreamRestClientFactory;
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
   * @return
   */
  public static ServerComponentHealthRestClient getClient(String dmsUri) {
    return FACTORY.getClient(dmsUri, Collections.emptyMap());
  }

  /**
   * Get a ServerComponentHealthRestClient with custom HTTP configs
   * @see BaseRestClientFactory#getClient(String, Map)
   * @param dmsUri URI to DMS endpoint
   * @param httpConfig custom config for HTTP client, please find the configs in
   *                   {@link com.linkedin.r2.transport.http.client.HttpClientFactory}
   * @return
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
