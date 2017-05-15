package com.linkedin.datastream;

import com.linkedin.r2.transport.common.Client;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;


public class DatrastreamRestClientFactory {
  private static Map<String, DatastreamRestClient> overrides = new HashMap<>();

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
   * Used mainly for testing, to override the DatastreamRestClient returned for a given dsmUri
   */
  public static void addOverride(String dsmUri, DatastreamRestClient restClient) {
    overrides.put(dsmUri, restClient);
  }

  public static  Map<String, DatastreamRestClient> getOverrides() {
    return Collections.unmodifiableMap(overrides);
  }
}
