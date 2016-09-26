package com.linkedin.datastream.server.d2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

import org.apache.commons.lang3.Validate;

import com.linkedin.datastream.common.VerifiableProperties;
import com.linkedin.datastream.server.Pair;


/**
 * Helper class to populate configs for DatastreamD2Manager.
 */
class DatastreamD2Config {
  public static final String DMS_D2_CLUSTER = "DatastreamService";

  public static final String ZK_SESSION_TIMEOUT = "zkSessionTimeout";
  public static final String ZK_RETRY_LIMIT = "zkRetryLimit";
  public static final String LB_STRATEGY_LIST = "loadBalancerStrategyList";
  public static final String PRIORITIZED_SCHEMES = "prioritizedSchemes";

  public static final int DEFAULT_ZK_SESSION_TIMEOUT = 30000; // 30s
  public static final int DEFAULT_ZK_RETRY_LIMIT = 100;

  enum EntryType { SINGLETON, LIST, MAP }
  private static final List<Pair<String, EntryType>> D2_SERVICE_ENTRIES;

  private final Map<String, Object> _serviceConfig = new HashMap<>();
  private final Map<String, Object> _clusterConfig = new HashMap<>();

  private final String _zkBasePath;
  private final int _zkSessionTimeout;
  private final int _zkRetryLimit;
  private final Map<String, String> _endpoints;

  /**
   * Support for parsing optional D2 configs.
   */
  static {
    D2_SERVICE_ENTRIES = new ArrayList<>();
    D2_SERVICE_ENTRIES.add(new Pair<>("loadBalancerStrategyList", EntryType.LIST));
    D2_SERVICE_ENTRIES.add(new Pair<>("prioritizedSchemes", EntryType.LIST));
    D2_SERVICE_ENTRIES.add(new Pair<>("loadBalancerStrategyProperties", EntryType.MAP));
    D2_SERVICE_ENTRIES.add(new Pair<>("transportClientProperties", EntryType.MAP));
    D2_SERVICE_ENTRIES.add(new Pair<>("degraderProperties", EntryType.MAP));
  }

  public DatastreamD2Config(Properties config, String clusterName, Map<String, String> endpoints) {
    Validate.notNull(config, "null D2 config");
    Validate.notBlank(clusterName, "invalid cluster name");
    Validate.notEmpty(endpoints, "invalid endpoints");

    VerifiableProperties props = new VerifiableProperties(config);
    _zkBasePath = String.format("/%s/d2", clusterName);
    _zkSessionTimeout = props.getInt(ZK_SESSION_TIMEOUT, DEFAULT_ZK_SESSION_TIMEOUT);
    _zkRetryLimit = props.getInt(ZK_RETRY_LIMIT, DEFAULT_ZK_RETRY_LIMIT);
    _endpoints = endpoints;

    // Populate mandatory service configs
    _serviceConfig.put(LB_STRATEGY_LIST, getSingletonList("degraderV3"));
    _serviceConfig.put(PRIORITIZED_SCHEMES, getSingletonList("http"));

    // Populate any optional service configs
    getOptServiceConfig(config);

    // Populate cluster configs
    Map<String, Object> svcMap = new HashMap<>();
    svcMap.put("services", _endpoints.entrySet().stream().collect(Collectors
        .toMap(e -> e.getKey(), e -> getSingletonMap("path", e.getValue()))));
    _clusterConfig.put(DMS_D2_CLUSTER, svcMap);
  }

  private void getOptServiceConfig(Properties config) {
    VerifiableProperties vp = new VerifiableProperties(config);
    for (Pair<String, EntryType> e : D2_SERVICE_ENTRIES) {
      if (e.getValue() == EntryType.LIST) {
        if (config.containsKey(e.getKey())) {
          _serviceConfig.put(e.getKey(), getList(config.getProperty(e.getKey())));
        }
      } else if (e.getValue() == EntryType.MAP) {
        Properties props = vp.getDomainProperties(e.getKey());
        Map<String, Object> map = new HashMap<>();
        if (props != null) {
          props.entrySet().forEach(s -> map.put((String) s.getKey(), s.getValue()));
        }
        _serviceConfig.put(e.getKey(), map);
      }
    }
  }

  /**
   * Create a mutable Map as required by D2Config.
   * @param key
   * @param value
   * @return
   */
  public static Map<String, String> getSingletonMap(String key, String value) {
    Map<String, String> ret = new HashMap<>();
    ret.put(key, value);
    return ret;
  }

  /**
   * Create a mutable List as required by D2Config.
   * @param value
   * @return
   */
  public List<String> getSingletonList(String value) {
    List<String> arr = new ArrayList<>();
    arr.add(value);
    return arr;
  }

  private static List<String> getList(String value) {
    return new ArrayList<>(Arrays.asList(value.split(",")));
  }

  public Map<String, Object> getServiceConfig() {
    return _serviceConfig;
  }

  public Map<String, Object> getClusterConfig() {
    return _clusterConfig;
  }

  public String getZkBasePath() {
    return _zkBasePath;
  }

  public int getSessionTimeout() {
    return _zkSessionTimeout;
  }

  public int getZkRetryLimit() {
    return _zkRetryLimit;
  }

  @SuppressWarnings("unchecked")
  public Map<String, Object> getClusterDefaults() {
    return (Map<String, Object>) Collections.EMPTY_MAP; // optional
  }

  @SuppressWarnings("unchecked")
  public Map<String, Object> getExtraClusterConfig() {
    return (Map<String, Object>) Collections.EMPTY_MAP; // optional
  }

  @SuppressWarnings("unchecked")
  public Map<String, Object> getServiceVariants() {
    return (Map<String, Object>) Collections.EMPTY_MAP; // optional
  }

  public static String getZkBasePath(String clusterName) {
    return String.format("/%s/d2", clusterName);
  }
}
