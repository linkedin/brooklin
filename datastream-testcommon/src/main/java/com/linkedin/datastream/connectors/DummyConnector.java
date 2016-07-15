package com.linkedin.datastream.connectors;

import java.util.List;
import java.util.Map;
import java.util.Properties;

import com.codahale.metrics.Metric;

import com.linkedin.datastream.common.Datastream;
import com.linkedin.datastream.server.DatastreamTask;
import com.linkedin.datastream.server.api.connector.Connector;
import com.linkedin.datastream.server.api.connector.DatastreamValidationException;


/**
 * A trivial implementation of connector interface
 */
public class DummyConnector implements Connector {

  public static final String VALID_DUMMY_SOURCE = "DummyConnector://DummySource";
  public static final String CONNECTOR_TYPE = "DummyConnector";
  public static final int NUM_PARTITIONS = 4;

  private Properties _properties;

  public DummyConnector(Properties properties) throws Exception {
    _properties = properties;
    String dummyConfigValue = _properties.getProperty("dummyProperty", "");
    if (!dummyConfigValue.equals("dummyValue")) {
      throw new Exception("Invalid config value for dummyProperty. Expected: dummyValue");
    }
  }

  @Override
  public void start() {
  }

  @Override
  public void stop() {
  }

  @Override
  public void onAssignmentChange(List<DatastreamTask> tasks) {

  }

  @Override
  public void initializeDatastream(Datastream stream, List<Datastream> allDatastreams)
      throws DatastreamValidationException {
    if (stream == null || stream.getSource() == null) {
      throw new DatastreamValidationException("Failed to get source from datastream.");
    }
    if (!stream.getSource().getConnectionString().equals(VALID_DUMMY_SOURCE)) {
      throw new DatastreamValidationException("Invalid source (" + stream.getSource() + ") in datastream.");
    }
    stream.getSource().setPartitions(NUM_PARTITIONS);
  }

  @Override
  public Map<String, Metric> getMetrics() {
    return null;
  }
}
