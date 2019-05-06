/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.server.diagnostics;

import java.lang.reflect.Method;
import java.time.Instant;
import java.util.concurrent.ConcurrentHashMap;

import com.codahale.metrics.MetricRegistry;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.linkedin.datastream.common.JsonUtils;
import com.linkedin.datastream.common.diag.KafkaPositionKey;
import com.linkedin.datastream.common.diag.KafkaPositionValue;
import com.linkedin.datastream.common.diag.PositionDataStore;
import com.linkedin.datastream.diagnostics.position.ConnectorPositionReport;
import com.linkedin.datastream.diagnostics.position.InstancePositionReport;
import com.linkedin.datastream.diagnostics.position.PositionData;
import com.linkedin.datastream.metrics.DynamicMetricsManager;
import com.linkedin.datastream.server.EmbeddedDatastreamCluster;
import com.linkedin.datastream.server.TestDatastreamServer;


/**
 * Tests {@link ConnectorPositionReportResource} backed by an {@link EmbeddedDatastreamCluster}.
 */
@Test(singleThreaded = true)
public class TestConnectorPositionReportResource {

  private EmbeddedDatastreamCluster _datastreamCluster;
  private String _testTaskPrefix;
  private KafkaPositionKey _testPositionKey;
  private KafkaPositionValue _testPositionValue;

  @BeforeMethod
  public void setUp(Method method) throws Exception {
    // Start the Brooklin cluster
    DynamicMetricsManager.createInstance(new MetricRegistry(), method.getName());
    _datastreamCluster = TestDatastreamServer.initializeTestDatastreamServerWithDummyConnector(null);
    _datastreamCluster.startup();

    // Clear any old position data from the store
    PositionDataStore.getInstance().clear();

    // Create test position data
    _testTaskPrefix = "testTaskPrefix";
    _testPositionKey = new KafkaPositionKey("testTopic", 0, "testHost", "testTask", Instant.ofEpochMilli(1L));
    final KafkaPositionValue positionValue = new KafkaPositionValue();
    positionValue.setBrokerOffset(2L);
    positionValue.setConsumerOffset(3L);
    positionValue.setAssignmentTime(Instant.ofEpochMilli(4L));
    positionValue.setLastRecordReceivedTimestamp(Instant.ofEpochMilli(5L));
    positionValue.setLastBrokerQueriedTime(Instant.ofEpochMilli(6L));
    positionValue.setLastNonEmptyPollTime(Instant.ofEpochMilli(7L));
    _testPositionValue = positionValue;

    // Add our test position data to the store
    PositionDataStore.getInstance()
        .computeIfAbsent(_testTaskPrefix, s -> new ConcurrentHashMap<>())
        .put(_testPositionKey, _testPositionValue);
  }

  @AfterMethod
  public void cleanUp() {
    _datastreamCluster.shutdown();
  }

  /**
   * Tests GET /connectorPositions
   */
  @Test
  public void testGet() {
    final ConnectorPositionReportResource resource =
        new ConnectorPositionReportResource(_datastreamCluster.getPrimaryDatastreamServer());
    final ConnectorPositionReport report = resource.get(false);

    // Check that the report was gathered successfully
    Assert.assertTrue(report.isSucceeded());

    // Check that the report only contains our queried server's data
    Assert.assertEquals(report.getPositionReports().size(), 1);

    // Check that the report data comes from our instance
    final InstancePositionReport instanceReport = report.getPositionReports().get(0);
    Assert.assertEquals(instanceReport.getInstance(), report.getRespondingInstance());

    // Check that the position data is correct
    Assert.assertEquals(instanceReport.getPositions().size(), 1);
    final PositionData position = instanceReport.getPositions().get(0);
    Assert.assertEquals(position.getTaskPrefix(), _testTaskPrefix);
    Assert.assertEquals(JsonUtils.fromJson(position.getKey(), _testPositionKey.getClass()), _testPositionKey);
    Assert.assertEquals(JsonUtils.fromJson(position.getValue(), _testPositionValue.getClass()), _testPositionValue);
  }

  /**
   * Tests GET /connectorPositions?aggregate=true
   */
  @Test
  public void testGetAll() {
    final ConnectorPositionReportResource resource =
        new ConnectorPositionReportResource(_datastreamCluster.getPrimaryDatastreamServer());
    final ConnectorPositionReport report = resource.get(true);

    // Check that the report was gathered successfully
    Assert.assertTrue(report.isSucceeded());

    // Check that the report contains data from all hosts in the cluster
    Assert.assertEquals(report.getPositionReports().size(), _datastreamCluster.getNumServers());

    // Check that the report data comes from every instance
    Assert.assertEquals(report.getPositionReports().stream().map(InstancePositionReport::getInstance).distinct().count(),
        _datastreamCluster.getNumServers());

    // Check that the position data is correct (it is shared among all instances since PositionDataStore is a singleton)
    report.getPositionReports().forEach(instanceReport -> {
      Assert.assertEquals(instanceReport.getPositions().size(), 1);
      final PositionData position = instanceReport.getPositions().get(0);
      Assert.assertEquals(position.getTaskPrefix(), _testTaskPrefix);
      Assert.assertEquals(JsonUtils.fromJson(position.getKey(), _testPositionKey.getClass()), _testPositionKey);
      Assert.assertEquals(JsonUtils.fromJson(position.getValue(), _testPositionValue.getClass()), _testPositionValue);
    });
  }
}