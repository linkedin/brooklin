package com.linkedin.datastream.connectors.oracle.triggerbased;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.linkedin.datastream.common.DatastreamRuntimeException;

public class TestOracleSource {

  @Test(expectedExceptions = DatastreamRuntimeException.class)
  public void testNull() {
    new OracleSource(null);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testWrongArgumentNumber() {
    new OracleSource("oracle:/dbName/viewName/extraName");
  }

  @Test(expectedExceptions = DatastreamRuntimeException.class)
  public void testWrongSchema() {
    new OracleSource("espresso:/dbName/viewName");
  }

  @Test
  public void testWithWhiteSpace() {
    OracleSource source = new OracleSource("oracle:/ dbName / viewName");
    Assert.assertEquals(source.getDbName(), "dbName");
    Assert.assertEquals(source.getViewName(), "viewName");
    Assert.assertEquals(source.getConnectionString(), "oracle:/dbName/viewName");
  }
}
