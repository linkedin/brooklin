package com.linkedin.datastream.server.d2;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.lang3.StringUtils;
import org.testng.Assert;
import org.testng.annotations.Test;

@Test
public class TestDatastreamD2Config {
  private static final Map<String, String> ENDPOINTS = new HashMap<>();

  static {
    ENDPOINTS.put("foo", "/foo");
    ENDPOINTS.put("bar", "/bar");
  }

  @SuppressWarnings("unchecked")
  private static void checkBasicConfig(DatastreamD2Config d2Config, String clusterName) {
    Assert.assertTrue(d2Config.getSessionTimeout() > 0);
    Assert.assertTrue(d2Config.getZkRetryLimit() > 0);
    Assert.assertNotNull(d2Config.getClusterConfig());
    Assert.assertNotNull(d2Config.getServiceConfig());
    Assert.assertTrue(d2Config.getServiceConfig().containsKey(DatastreamD2Config.LB_STRATEGY_LIST));
    Assert.assertTrue(d2Config.getServiceConfig().containsKey(DatastreamD2Config.PRIORITIZED_SCHEMES));

    String basePath = String.format("/%s/d2", clusterName);
    Assert.assertEquals(d2Config.getZkBasePath(), basePath);

    String d2Cluster = DatastreamD2Config.DMS_D2_CLUSTER;
    Assert.assertNotNull(d2Config.getClusterConfig().get(d2Cluster));
    Map<String, Object> svcMap = (Map<String, Object>) d2Config.getClusterConfig().get(d2Cluster);
    Map<String, Object> epMap = (Map<String, Object>) svcMap.get("services");

    ENDPOINTS.entrySet().forEach(e -> {
      String expected = StringUtils.prependIfMissing(e.getValue(), "/");
      String actual = (String) ((Map<String, Object>) epMap.get(e.getKey())).get("path");
      Assert.assertEquals(actual, expected);
    });
  }

  @Test
  public void testHappyPathEmptyProperties() {
    String clusterName = "testHappyPathEmptyProperties";
    Properties props = new Properties();
    DatastreamD2Config d2Config = new DatastreamD2Config(props, clusterName, ENDPOINTS);
    checkBasicConfig(d2Config, clusterName);
  }

  @Test
  public void testHappyPathRealProperties() {
    String clusterName = "testHappyPathRealProperties";
    Properties props = new Properties();
    props.put(DatastreamD2Config.ZK_SESSION_TIMEOUT, "12345");
    props.put(DatastreamD2Config.ZK_RETRY_LIMIT, "222");

    DatastreamD2Config d2Config = new DatastreamD2Config(props, clusterName, ENDPOINTS);
    Assert.assertEquals(d2Config.getSessionTimeout(), 12345);
    Assert.assertEquals(d2Config.getZkRetryLimit(), 222);

    checkBasicConfig(d2Config, clusterName);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testHappyPathOptionalServiceProperties() {
    String clusterName = "testHappyPathOptClusterProperties";
    Properties props = new Properties();
    props.put("loadBalancerStrategyProperties.http.loadBalancer.updateIntervalMs", "5000");
    props.put("loadBalancerStrategyProperties.http.loadBalancer.pointsPerWeight", "100");
    props.put("transportClientProperties.http.requestTimeout", "10000");
    props.put("degraderProperties.degrader.minCallCount", "10");
    props.put("degraderProperties.degrader.lowErrorRate", "0.01");
    props.put("degraderProperties.degrader.highErrorRate", "0.1");

    DatastreamD2Config d2Config = new DatastreamD2Config(props, clusterName, ENDPOINTS);
    checkBasicConfig(d2Config, clusterName);

    Map<String, Object> service = d2Config.getServiceConfig();
    Assert.assertNotNull(service.get("loadBalancerStrategyProperties"));
    Map<String, Object> lbProps = (Map<String, Object>) service.get("loadBalancerStrategyProperties");
    Assert.assertEquals(lbProps.get("http.loadBalancer.updateIntervalMs"), "5000");
    Assert.assertEquals(lbProps.get("http.loadBalancer.pointsPerWeight"), "100");

    Assert.assertNotNull(service.get("transportClientProperties"));
    Map<String, Object> txProps = (Map<String, Object>) service.get("transportClientProperties");
    Assert.assertEquals(txProps.get("http.requestTimeout"), "10000");

    Assert.assertNotNull(service.get("degraderProperties"));
    Map<String, Object> dgProps = (Map<String, Object>) service.get("degraderProperties");
    Assert.assertEquals(dgProps.get("degrader.minCallCount"), "10");
    Assert.assertEquals(dgProps.get("degrader.lowErrorRate"), "0.01");
    Assert.assertEquals(dgProps.get("degrader.highErrorRate"), "0.1");
  }
}
