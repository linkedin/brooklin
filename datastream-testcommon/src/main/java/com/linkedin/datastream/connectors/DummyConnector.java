/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.connectors;

import java.util.List;
import java.util.Map;
import java.util.Properties;

import com.linkedin.datastream.common.Datastream;
import com.linkedin.datastream.common.DatastreamConstants;
import com.linkedin.datastream.common.DiagnosticsAware;
import com.linkedin.datastream.metrics.BrooklinMetricInfo;
import com.linkedin.datastream.server.DatastreamTask;
import com.linkedin.datastream.server.api.connector.Connector;
import com.linkedin.datastream.server.api.connector.DatastreamValidationException;
import com.linkedin.datastream.server.providers.CheckpointProvider;


/**
 * A trivial implementation of the {@link Connector} interface
 */
public class DummyConnector implements Connector, DiagnosticsAware {

  public static final String VALID_DUMMY_SOURCE = "DummyConnector://DummySource";
  public static final String CONNECTOR_TYPE = "DummyConnector";

  private final Properties _properties;

  /**
   * Constructor for DummyConnector
   * @param properties Configuration properties
   * @throws Exception if {@code properties} does not contain a property
   *         whose name is "dummyProperty" and value is "dummyValue"
   */
  public DummyConnector(Properties properties) throws Exception {
    _properties = properties;
    String dummyConfigValue = _properties.getProperty("dummyProperty", "");
    if (!dummyConfigValue.equals("dummyValue")) {
      throw new Exception("Invalid config value for dummyProperty. Expected: dummyValue");
    }
  }

  @Override
  public void start(CheckpointProvider checkpointProvider) {
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
  }

  @Override
  public void validateAndUpdateDatastreams(List<Datastream> datastreams, List<Datastream> allDatastreams)
      throws DatastreamValidationException {
  }

  @Override
  public List<BrooklinMetricInfo> getMetricInfos() {
    return null;
  }

  @Override
  public String process(String query) {
    return "HEALTHY";
  }

  @Override
  public String reduce(String query, Map<String, String> responses) {
    return "DummyStatus";
  }

  @Override
  public boolean isDatastreamUpdateTypeSupported(Datastream datastream, DatastreamConstants.UpdateType updateType) {
    return DatastreamConstants.UpdateType.PAUSE_RESUME_PARTITIONS == updateType;
  }
}
