package com.linkedin.datastream;

import com.linkedin.r2.transport.common.Client;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Factory class for creating  {@link DatastreamRestClient} objects.
 * This factory is needed to allow mocking the DatastreamRestClient during test.
 *
 * The reason is that currently we inject the dmsUri as part of the configuration
 * but we don't have a dependency injection framework that allows us to inject mock
 * DatastreamRestClient, without doing a major refactoring of the code.
 */
public class DatastreamRestClientFactory {
  private static Map<String, DatastreamRestClient> overrides = new ConcurrentHashMap<>();

  public static DatastreamRestClient getClient(String dsmUri) {
    if (overrides.containsKey(dsmUri)) {
      return overrides.get(dsmUri);
    }
    return new DatastreamRestClient(dsmUri);
  }

  public static DatastreamRestClient getClient(String dsmUri, Client r2Client) {
    if (overrides.containsKey(dsmUri)) {
      return overrides.get(dsmUri);
    }
    return new DatastreamRestClient(dsmUri, r2Client);
  }

  /**
   * Used mainly for testing, to override the DatastreamRestClient returned for a given dsmUri.
   * Note that this is an static method, each test case should use its own dsmUri, to avoid conflicts
   * in case the test are running in parallel.
   */
  public static void addOverride(String dsmUri, DatastreamRestClient restClient) {
    overrides.put(dsmUri, restClient);
  }

  public static Map<String, DatastreamRestClient> getOverrides() {
    return Collections.unmodifiableMap(overrides);
  }
}
