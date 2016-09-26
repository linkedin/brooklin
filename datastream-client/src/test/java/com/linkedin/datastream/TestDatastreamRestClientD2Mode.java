package com.linkedin.datastream;

import org.testng.annotations.Test;

/**
 * Test DatastreamRestClient in D2Mode.
 */
@Test(singleThreaded = true)
public class TestDatastreamRestClientD2Mode extends TestDatastreamRestClient {
  public DatastreamRestClient createRestClient() {
    return new DatastreamRestClient(_embeddedZookeeper.getConnection(),
        _datastreamServer.getCoordinator().getClusterName());
  }
}
