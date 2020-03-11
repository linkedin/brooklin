/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.connectors;

import java.util.HashMap;
import java.util.List;
import java.util.Properties;

import com.linkedin.datastream.common.Datastream;
import com.linkedin.datastream.metrics.BrooklinMetricInfo;
import com.linkedin.datastream.server.DatastreamTask;
import com.linkedin.datastream.server.api.connector.Connector;
import com.linkedin.datastream.server.api.connector.DatastreamValidationException;
import com.linkedin.datastream.server.providers.CheckpointProvider;


/**
 * A trivial implementation of the {@link Connector} interface
 */
public class DummyBootstrapConnector implements Connector {

  public static final String CONNECTOR_NAME = "DummyConnectorBootstrap";

  final Map<String, String> _config;

  /**
   * Constructor for DummyBootstrapConnector
   */
  public DummyBootstrapConnector(Properties properties) throws Exception {

    _config = new HashMap<>();
    for (final String name : properties.stringPropertyNames()) {
      _config.put(name, properties.getProperty(name));
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

    if (!stream.hasMetadata()) {
      stream.getMetadata().putAll(_config);
    }
  }

  @Override
  public List<BrooklinMetricInfo> getMetricInfos() {
    return null;
  }
}
