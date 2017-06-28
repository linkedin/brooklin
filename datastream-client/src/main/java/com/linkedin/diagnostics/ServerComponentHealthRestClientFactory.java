package com.linkedin.diagnostics;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.linkedin.datastream.common.RestliUtils;
import com.linkedin.restli.client.RestClient;


/**
 * ServerComponentHealth Resource Factory that is used to create the ServerComponentHealth restli resources.
 */
public class ServerComponentHealthRestClientFactory {

  private static Map<String, ServerComponentHealthRestClient> overrides = new ConcurrentHashMap<>();

  public static ServerComponentHealthRestClient getClient(String dmsUri) {
    dmsUri = RestliUtils.sanitizeUri(dmsUri);
    if (overrides.containsKey(dmsUri)) {
      return overrides.get(dmsUri);
    }
    return new ServerComponentHealthRestClient(dmsUri);
  }

  /**
   * Get a ServerComponentHealthRestClient with default HTTP client and custom HTTP configs
   * @param dmsUri URI to DMS endpoint
   * @param httpConfig custom config for HTTP client, please find the configs in {@link com.linkedin.r2.transport.http.client.HttpClientFactory}
   * @return
   */
  public static ServerComponentHealthRestClient getClient(String dmsUri, Map<String, String> httpConfig) {
    dmsUri = RestliUtils.sanitizeUri(dmsUri);
    if (overrides.containsKey(dmsUri)) {
      return overrides.get(dmsUri);
    }
    return new ServerComponentHealthRestClient(dmsUri, httpConfig);
  }

  /**
   * Get a ServerComponentHealthRestClient with an existing Rest.li RestClient (must bound to the same dmsUri).
   * @param dmsUri URI to DMS endpoint
   * @param restClient Rest.li RestClient
   * @return
   */
  public static ServerComponentHealthRestClient getClient(String dmsUri, RestClient restClient) {
    dmsUri = RestliUtils.sanitizeUri(dmsUri);
    if (overrides.containsKey(dmsUri)) {
      return overrides.get(dmsUri);
    }
    return new ServerComponentHealthRestClient(restClient);
  }

  /**
   * Used mainly for testing, to override the ServerComponentHealthRestClient returned for a given dmsUri.
   * Note that this is an static method, each test case should use its own dmsUri, to avoid conflicts
   * in case the test are running in parallel.
   */
  public static void addOverride(String dmsUri, ServerComponentHealthRestClient restClient) {
    dmsUri = RestliUtils.sanitizeUri(dmsUri);
    overrides.put(dmsUri, restClient);
  }

  /**
   * @return the current RestClient override mappings
   */
  public static Map<String, ServerComponentHealthRestClient> getOverrides() {
    return Collections.unmodifiableMap(overrides);
  }
}
