package com.linkedin.datastream.connectors.oracle.triggerbased;

import java.util.Properties;

import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import com.codahale.metrics.MetricRegistry;

import com.linkedin.datastream.common.Datastream;
import com.linkedin.datastream.common.DatastreamException;
import com.linkedin.datastream.common.DatastreamSource;
import com.linkedin.datastream.common.SchemaRegistryClient;
import com.linkedin.datastream.common.DynamicDataSourceFactoryImpl;
import com.linkedin.datastream.metrics.DynamicMetricsManager;
import com.linkedin.datastream.server.api.connector.Connector;
import com.linkedin.datastream.server.api.connector.DatastreamValidationException;
import com.linkedin.datastream.connectors.oracle.triggerbased.consumer.OracleConsumerConfig;


@Test
public class TestOracleConnector {

  @BeforeTest
  public void setUp() {
    DynamicMetricsManager.createInstance(new MetricRegistry());
  }

  @Test
  public void testOracleConnectorConfig() throws DatastreamException {

    String className = DynamicDataSourceFactoryImpl.class.getName();

    Properties prop = new Properties();
    prop.put("schemaRegistryName", "randomSchemaRegistry");
    prop.put("schemaRegistry.randomSchemaRegistry.mode", "randomMode");
    prop.put("schemaRegistry.randomSchemaRegistry.uri", "uri");
    prop.put("oracleConsumer.dbName.dbUri", "uri");
    prop.put(OracleConnectorConfig.DAEMON_THREAD_INTERVAL_SECONDS, "3");

    OracleConnectorConfig config = new OracleConnectorConfig(prop);
    OracleConsumerConfig consumerConfig = config.getOracleConsumerConfig("dbName");

    Assert.assertEquals(config.getSchemaRegistryConfig().getProperty("mode"), "randomMode");
    Assert.assertEquals(config.getSchemaRegistryConfig().getProperty("uri"), "uri");
    Assert.assertEquals(config.getDaemonThreadIntervalSeconds(), 3);
    Assert.assertEquals(consumerConfig.getDbUri(), "uri");
    Assert.assertEquals(consumerConfig.getDataSourceFactoryClass(), className);
  }

  @Test
  public void testInitializeDatastream() throws Exception {
    Properties props = OracleConnectorTestUtils.getDefaultProps();
    OracleConnectorConfig config = new OracleConnectorConfig(props);

    // register a mock schema in the in-memory schema registry and store the returned Id
    SchemaRegistryClient client =
        new SchemaRegistryFactoryImpl().createSchemaRegistryClient(config.getSchemaRegistryConfig());
    String id = client.registerSchema(MockSchema.GENERIC_SCHEMA.getName(), MockSchema.GENERIC_SCHEMA);

    // creating the Oracle Connector
    Connector connector =
        new OracleConnectorFactory().createConnector(OracleConnectorTestUtils.DEFAULT_CONNECTOR_NAME, props);

    // creating the Datastream
    Datastream datastream = OracleConnectorTestUtils.createDatastream("oracle:/dbName/viewName", id, "connString");

    connector.initializeDatastream(datastream, null);
    Assert.assertEquals((int) datastream.getSource().getPartitions(), 1);
  }

  @Test(expectedExceptions = DatastreamValidationException.class)
  public void testInvalidInitializeDatastream() throws Exception {

    Properties prop = OracleConnectorTestUtils.getDefaultProps();
    Connector connector =
        new OracleConnectorFactory().createConnector(OracleConnectorTestUtils.DEFAULT_CONNECTOR_NAME, prop);

    // creating the Datastream
    Datastream datastream = OracleConnectorTestUtils.createDatastream();

    // setting the Oracle Source for the datastream
    DatastreamSource source = new DatastreamSource();
    source.setConnectionString("oracle:/dbName/viewName");
    datastream.setSource(source);

    // throw error since it does not supply a SCHEMA_ID
    connector.initializeDatastream(datastream, null);
  }
}

