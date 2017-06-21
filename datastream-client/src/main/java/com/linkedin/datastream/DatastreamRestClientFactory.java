package com.linkedin.datastream;

import com.linkedin.r2.transport.common.Client;
import com.linkedin.restli.client.RestClient;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.commons.lang3.StringUtils;


/**
 * Factory class for creating  {@link DatastreamRestClient} objects.
 * This factory is needed to allow mocking the DatastreamRestClient during test.
 *
 * The reason is that currently we inject the dmsUri as part of the configuration
 * but we don't have a dependency injection framework that allows us to inject mock
 * DatastreamRestClient, without doing a major refactoring of the code.
 */
public class DatastreamRestClientFactory {
  private static final String DEFAULT_URI_SCHEME = "http://";

  private static Map<String, DatastreamRestClient> overrides = new ConcurrentHashMap<>();

  private static String sanitizeUri(String dmsUri) {
    return StringUtils.prependIfMissing(StringUtils.appendIfMissing(dmsUri, "/"), DEFAULT_URI_SCHEME);
  }

  /**
   * Get a DatastreamRestClient with default HTTP client
   * @param dmsUri URI to DMS endpoint
   * @return
   */
  public static DatastreamRestClient getClient(String dmsUri) {
    return getClient(dmsUri, Collections.<String, String>emptyMap());
  }

  /**
   * Get a DatastreamRestClient with default HTTP client and custom HTTP configs
   * @param dmsUri URI to DMS endpoint
   * @param httpConfig custom config for HTTP client, please find the configs in {@link com.linkedin.r2.transport.http.client.HttpClientFactory}
   * @return
   */
  public static DatastreamRestClient getClient(String dmsUri, Map<String, String> httpConfig) {
    dmsUri = sanitizeUri(dmsUri);
    if (overrides.containsKey(dmsUri)) {
      return overrides.get(dmsUri);
    }
    return new DatastreamRestClient(dmsUri, httpConfig);
  }

  /**
   * Get a DatastreamRestClient with an existing Rest.li R2Client
   * @param dmsUri URI to DMS endpoint
   * @param r2Client Rest.li R2Client
   * @return
   */
  public static DatastreamRestClient getClient(String dmsUri, Client r2Client) {
    dmsUri = sanitizeUri(dmsUri);
    if (overrides.containsKey(dmsUri)) {
      return overrides.get(dmsUri);
    }
    return new DatastreamRestClient(dmsUri, r2Client);
  }

  /**
   * Get a DatastreamRestClient with an existing Rest.li RestClient (must bound to the same dmsUri).
   * @param dmsUri URI to DMS endpoint
   * @param restClient Rest.li RestClient
   * @return
   */
  public static DatastreamRestClient getClient(String dmsUri, RestClient restClient) {
    dmsUri = sanitizeUri(dmsUri);
    if (overrides.containsKey(dmsUri)) {
      return overrides.get(dmsUri);
    }
    return new DatastreamRestClient(restClient);
  }

  /**
   * Used mainly for testing, to override the DatastreamRestClient returned for a given dmsUri.
   * Note that this is an static method, each test case should use its own dmsUri, to avoid conflicts
   * in case the test are running in parallel.
   */
  public static void addOverride(String dmsUri, DatastreamRestClient restClient) {
    dmsUri = sanitizeUri(dmsUri);
    overrides.put(dmsUri, restClient);
  }

  /**
   * @return the current RestClient override mappings
   */
  public static Map<String, DatastreamRestClient> getOverrides() {
    return Collections.unmodifiableMap(overrides);
  }
}
