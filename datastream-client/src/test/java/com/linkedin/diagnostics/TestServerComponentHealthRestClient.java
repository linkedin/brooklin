/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.diagnostics;

import org.apache.log4j.Level;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import com.linkedin.TestRestliClientBase;
import com.linkedin.datastream.diagnostics.ServerComponentHealth;


@Test(singleThreaded = true)
public class TestServerComponentHealthRestClient extends TestRestliClientBase {

  private static final Logger LOG = LoggerFactory.getLogger(TestServerComponentHealthRestClient.class);

  @BeforeTest
  public void setUp() throws Exception {
    org.apache.log4j.Logger.getRootLogger().setLevel(Level.INFO);

    // Create a cluster with maximum 2 DMS instances
    setupDatastreamCluster(2);
  }

  @AfterTest
  public void tearDown() throws Exception {
    _datastreamCluster.shutdown();
  }

  /**
   * Create a rest client with the default/leader DMS instance
   * @return
   */
  private ServerComponentHealthRestClient createRestClient() {
    String dmsUri = String.format("http://localhost:%d", _datastreamCluster.getDatastreamPorts().get(0));
    return ServerComponentHealthRestClientFactory.getClient(dmsUri);
  }

  @Test
  public void testGetStatus() throws Exception {
    // happy path test case
    String name = "Connector";
    String type = "DummyConnector";
    String content = "topic=datastream";
    String expectedStatus = "HEALTHY";
    ServerComponentHealthRestClient restClient = createRestClient();
    ServerComponentHealth response =  restClient.getStatus(name, type, content);

    Assert.assertEquals(response.isSucceeded(), Boolean.TRUE);
    Assert.assertEquals(response.getStatus(), expectedStatus);
    Assert.assertTrue(response.getErrorMessages().isEmpty());

    // invalid name, null response.
    name = "NonExistComponent";
    response = restClient.getStatus(name, type, content);
    Assert.assertEquals(response, null);

    // invalid type, null response.
    type = "NonExistConnector";
    response = restClient.getStatus(name, type, content);
    Assert.assertEquals(response, null);

    // Connector throws exception, null response
    type = "BrokenConnector";
    response = restClient.getStatus(name, type, content);
    Assert.assertEquals(response, null);
  }

  @Test
  public void testGetAllStatus() throws Exception {
    // happy path test case
    String name = "Connector";
    String type = "DummyConnector";
    String content = "topic=datastream";
    String expectedStatus = "DummyStatus";
    ServerComponentHealthRestClient restClient = createRestClient();
    ServerComponentHealth response =  restClient.getAllStatus(name, type, content);

    Assert.assertEquals(response.isSucceeded(), Boolean.TRUE);
    Assert.assertEquals(response.getStatus(), expectedStatus);
    Assert.assertEquals(response.getErrorMessages(), "{}");

    // invalid name, null response.
    name = "NonExistComponent";
    response = restClient.getAllStatus(name, type, content);
    Assert.assertEquals(response, null);

    // invalid type, null response.
    type = "NonExistConnector";
    response = restClient.getAllStatus(name, type, content);
    Assert.assertEquals(response, null);

    // Connector throws exception, null response
    type = "BrokenConnector";
    response = restClient.getAllStatus(name, type, content);
    Assert.assertEquals(response, null);
  }
}
