package com.linkedin.datastream.server.diagnostics;

import java.util.List;

import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.codahale.metrics.MetricRegistry;

import com.linkedin.datastream.diagnostics.ServerComponentHealth;
import com.linkedin.datastream.metrics.DynamicMetricsManager;
import com.linkedin.datastream.server.EmbeddedDatastreamCluster;
import com.linkedin.datastream.server.TestDatastreamServer;
import com.linkedin.restli.server.PagingContext;


/**
 * Test ServerComponentHealthResources with zookeeper backed DatastreamStore
 */
@Test(singleThreaded = true)
public class TestServerComponentHealthResources {

  private static final PagingContext NO_PAGING = new PagingContext(0, 0, false, false);

  private EmbeddedDatastreamCluster _datastreamKafkaCluster;

  @BeforeMethod
  public void setUp()
      throws Exception {
    DynamicMetricsManager.createInstance(new MetricRegistry());
    _datastreamKafkaCluster = TestDatastreamServer.initializeTestDatastreamServerWithDummyConnector(null);
    _datastreamKafkaCluster.startup();
  }

  @AfterMethod
  public void cleanup() {
    _datastreamKafkaCluster.shutdown();
  }

  @Test
  public void testGetStatus() {
    ServerComponentHealthResources resource =
        new ServerComponentHealthResources(_datastreamKafkaCluster.getPrimaryDatastreamServer());

    String name = "Connector";
    String type = "DummyConnector";
    String content = "topic=datastream";
    String expectedStatus = "HEALTHY";

    List<ServerComponentHealth> response = resource.getStatus(NO_PAGING, name, type, content);
    for (ServerComponentHealth sch : response) {
      //Assert.assertEquals(sch.getName(), type + name);
      Assert.assertEquals(sch.getStatus(), expectedStatus);
      //Assert.assertEquals(sch.getStatusDetail().get(0), content);
    }

  }
}
