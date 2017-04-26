package com.linkedin.datastream.connectors.oracle.triggerbased.consumer;

import java.util.Properties;

import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.linkedin.datastream.common.DatastreamException;
import com.linkedin.datastream.connectors.oracle.triggerbased.MockSchema;
import com.linkedin.datastream.connectors.oracle.triggerbased.OracleSource;


@Test
public class TestOracleConsumer {
  private OracleConsumer _consumer;
  private OracleSource _source;

  @BeforeClass
  public void setup() throws DatastreamException {
    Properties prop = new Properties();
    prop.put("dbUri", "random");

    OracleConsumerConfig config = new OracleConsumerConfig(prop);

    String connStr = "oracle:/dbName/viewName";

    _source = new OracleSource(connStr);
    _consumer = new OracleConsumer(config, _source, MockSchema.GENERIC_SCHEMA);
  }

  @Test
  public void testGetSafeUri() {
    Assert.assertEquals(_consumer.getName(), _source.getConnectionString());
  }

  @Test
  public void testEventQueryString() {
    String sql = OracleConsumer.generateEventQuery(_source, "/*+ first_rows LEADING(tx) +*/");
    String expectedSql = "select /*+ first_rows LEADING(tx) +*/ tx.scn scn, tx.ts event_timestamp, src.*"
        + " from viewName src, sy$txlog tx where src.txn=tx.txn and tx.scn > ? and tx.scn < ?";

    Assert.assertEquals(sql, expectedSql);
  }

  @Test
  public void testMaxScnQueryString() {
    String sql = OracleConsumer.generateMaxScnQuery();
    String expectedSql = "select max(scn) from sy$txlog";

    Assert.assertEquals(sql, expectedSql);
  }
}