/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.server;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.log4j.Level;
import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import com.linkedin.datastream.connectors.DummyConnectorFactory;
import com.linkedin.datastream.diagnostics.ServerHealth;
import com.linkedin.datastream.server.diagnostics.HealthRequestBuilders;
import com.linkedin.datastream.testutil.DatastreamEmbeddedZookeeperKafkaCluster;
import com.linkedin.r2.RemoteInvocationException;
import com.linkedin.r2.transport.common.Client;
import com.linkedin.r2.transport.common.bridge.client.TransportClientAdapter;
import com.linkedin.r2.transport.http.client.HttpClientFactory;
import com.linkedin.restli.client.GetRequest;
import com.linkedin.restli.client.Response;
import com.linkedin.restli.client.ResponseFuture;
import com.linkedin.restli.client.RestClient;


/**
 * Tests for {@link ServerHealth}
 */
public class TestServerHealth {

  private EmbeddedDatastreamCluster _datastreamCluster;
  private HealthRequestBuilders _builders;
  private RestClient _restClient;

  @BeforeTest
  public void setUp() throws Exception {
    org.apache.log4j.Logger.getRootLogger().setLevel(Level.INFO);
    _datastreamCluster = initializeTestDatastreamServer(null);
    _datastreamCluster.startup();
  }

  /**
   * Create an embedded datastream cluster initialized with {@value TestDatastreamServer#DUMMY_CONNECTOR}
   * @param override Configuration properties to use to override defaults
   */
  public static EmbeddedDatastreamCluster initializeTestDatastreamServer(Properties override) throws Exception {
    Map<String, Properties> connectorProperties = new HashMap<>();
    connectorProperties.put(TestDatastreamServer.DUMMY_CONNECTOR, getDummyConnectorProperties(false));
    return EmbeddedDatastreamCluster.newTestDatastreamCluster(new DatastreamEmbeddedZookeeperKafkaCluster(),
        connectorProperties, override);
  }

  private static Properties getDummyConnectorProperties(boolean bootstrap) {
    Properties props = new Properties();
    props.put(DatastreamServerConfigurationConstants.CONFIG_CONNECTOR_ASSIGNMENT_STRATEGY_FACTORY,
        TestDatastreamServer.BROADCAST_STRATEGY_FACTORY);
    props.put(DatastreamServerConfigurationConstants.CONFIG_FACTORY_CLASS_NAME,
        DummyConnectorFactory.class.getTypeName());
    if (bootstrap) {
      props.put(DatastreamServerConfigurationConstants.CONFIG_CONNECTOR_BOOTSTRAP_TYPE,
          TestDatastreamServer.DUMMY_BOOTSTRAP_CONNECTOR);
    }
    props.put("dummyProperty", "dummyValue");
    return props;
  }

  @AfterTest
  public void tearDown() throws Exception {
    _datastreamCluster.shutdown();
  }

  @Test
  public void testServerHealthHasRightClusterNameAndInstanceName() throws RemoteInvocationException {
    ServerHealth serverHealth = fetchServerHealth();
    Assert.assertEquals(serverHealth.getClusterName(), _datastreamCluster.getDatastreamServerProperties()
        .get(0)
        .getProperty(DatastreamServerConfigurationConstants.CONFIG_CLUSTER_NAME));
    Assert.assertEquals(serverHealth.getInstanceName(),
        _datastreamCluster.getPrimaryDatastreamServer().getCoordinator().getInstanceName());
  }

  private ServerHealth fetchServerHealth() throws RemoteInvocationException {
    String healthUri = "http://localhost:" + _datastreamCluster.getDatastreamPorts().get(0) + "/";
    _builders = new HealthRequestBuilders();
    final HttpClientFactory http = new HttpClientFactory();
    final Client r2Client = new TransportClientAdapter(http.getClient(Collections.<String, String>emptyMap()));
    _restClient = new RestClient(r2Client, healthUri);
    GetRequest<ServerHealth> request = _builders.get().build();
    ResponseFuture<ServerHealth> healthResponse = _restClient.sendRequest(request);

    Response<ServerHealth> response;
    response = healthResponse.getResponse();
    return response.getEntity();
  }
}
