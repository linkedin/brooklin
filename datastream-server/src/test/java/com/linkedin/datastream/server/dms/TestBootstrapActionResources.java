package com.linkedin.datastream.server.dms;

import com.linkedin.datastream.common.Datastream;
import com.linkedin.datastream.server.TestDatastreamServer;
import com.linkedin.datastream.connectors.DummyBootstrapConnector;
import com.linkedin.datastream.server.EmbeddedDatastreamCluster;
import com.linkedin.restli.server.RestLiServiceException;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Test BootstrapActionResources with zookeeper backed DatastreamStore
 */
@Test(singleThreaded=true)
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
    DatastreamResources datastreamResources = new DatastreamResources();
    BootstrapActionResources bootstrapActionResource = new BootstrapActionResources();

    boolean exceptionCaught = false;
    try {
      bootstrapActionResource.create("name_1");
    } catch (RestLiServiceException e) {
      exceptionCaught = true;
    }
    Assert.assertTrue(exceptionCaught);

    Datastream onlineDatastream = TestDatastreamResources.generateDatastream(1);
    datastreamResources.create(onlineDatastream);
    Datastream bootstrapDatastream = bootstrapActionResource.create(onlineDatastream.getName());
    Assert.assertNotNull(bootstrapDatastream);
    Assert.assertEquals(bootstrapDatastream.getSource(), onlineDatastream.getSource());
    Assert.assertEquals(bootstrapDatastream.getConnectorType(), DummyBootstrapConnector.CONNECTOR_TYPE);
  }
}
