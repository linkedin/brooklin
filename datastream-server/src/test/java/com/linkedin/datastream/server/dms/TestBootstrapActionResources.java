package com.linkedin.datastream.server.dms;

import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.linkedin.datastream.common.Datastream;
import com.linkedin.datastream.server.EmbeddedDatastreamCluster;
import com.linkedin.datastream.server.TestDatastreamServer;


/**
 * Test BootstrapActionResources with zookeeper backed DatastreamStore
 */
@Test(singleThreaded = true)
public class TestBootstrapActionResources {

  private EmbeddedDatastreamCluster _datastreamKafkaCluster;

  @BeforeMethod
  public void setUp() throws Exception {
    _datastreamKafkaCluster = TestDatastreamServer.initializeTestDatastreamServerWithBootstrap();
    _datastreamKafkaCluster.startup();
  }

  @AfterMethod
  public void cleanup() {
    _datastreamKafkaCluster.shutdown();
  }

  @Test
  public void testCreateBootstrapDatastream() throws Exception {
    BootstrapActionResources bootstrapActionResource = new BootstrapActionResources(
        _datastreamKafkaCluster.getPrimaryDatastreamServer());
    Datastream bootstrapDatastream = bootstrapActionResource.create(TestDatastreamResources.generateDatastream(1));
    Assert.assertNotNull(bootstrapDatastream);
  }
}
