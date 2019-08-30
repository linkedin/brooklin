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
import com.linkedin.datastream.common.DatastreamRuntimeException;
import com.linkedin.datastream.common.DiagnosticsAware;
import com.linkedin.datastream.metrics.BrooklinMetricInfo;
import com.linkedin.datastream.server.DatastreamTask;
import com.linkedin.datastream.server.api.connector.Connector;
import com.linkedin.datastream.server.api.connector.DatastreamValidationException;
import com.linkedin.datastream.server.providers.CheckpointProvider;


/**
 * A trivial implementation of the {@link Connector} interface
 */
public class BrokenConnector implements Connector, DiagnosticsAware {

  public static final String VALID_DUMMY_SOURCE = "BrokenConnector://DummySource";
  public static final String CONNECTOR_TYPE = "BrokenConnector";

  private final Properties _properties;

  /**
   * Constructor for BrokenConnector
   * @param properties Configuration properties
   * @throws Exception if {@code properties} does not contain a property
   *         whose name is "dummyProperty" and value is "dummyValue"
   */
  public BrokenConnector(Properties properties) throws Exception {
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
  public void validateUpdateDatastreams(List<Datastream> datastreams, List<Datastream> allDatastreams)
      throws DatastreamValidationException {
  }

  @Override
  public List<BrooklinMetricInfo> getMetricInfos() {
    return null;
  }

  @Override
  public String process(String query) {
    throw new DatastreamRuntimeException("Exception in process function of connector");
  }

  @Override
  public String reduce(String query, Map<String, String> responses) {
    throw new DatastreamRuntimeException("Exception in reduce function of connector");
  }
}
