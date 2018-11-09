package com.linkedin;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import com.linkedin.datastream.common.DatastreamException;
import com.linkedin.datastream.connectors.DummyConnector;
import com.linkedin.datastream.connectors.DummyConnectorFactory;
import com.linkedin.datastream.server.DatastreamServerConfigurationConstants;
import com.linkedin.datastream.server.EmbeddedDatastreamCluster;
import com.linkedin.datastream.server.assignment.BroadcastStrategyFactory;


public class TestRestliClientBase {

  public static final String TRANSPORT_NAME = "default";
  public static final long WAIT_TIMEOUT_MS = Duration.ofMinutes(3).toMillis();
  public EmbeddedDatastreamCluster _datastreamCluster;

  public void setupDatastreamCluster(int numServers) throws IOException, DatastreamException {
    Properties connectorProps = new Properties();
    connectorProps.put(DatastreamServerConfigurationConstants.CONFIG_FACTORY_CLASS_NAME, DummyConnectorFactory.class.getCanonicalName());
    connectorProps.put(DatastreamServerConfigurationConstants.CONFIG_CONNECTOR_ASSIGNMENT_STRATEGY_FACTORY,
        BroadcastStrategyFactory.class.getTypeName());
    connectorProps.put("dummyProperty", "dummyValue");

    _datastreamCluster = EmbeddedDatastreamCluster.newTestDatastreamCluster(
        Collections.singletonMap(DummyConnector.CONNECTOR_TYPE, connectorProps), null, numServers);

    // NOTE: Only start the first instance by default
    // Test case needing the 2nd one should start the 2nd instance
    _datastreamCluster.startupServer(0);
  }

}
