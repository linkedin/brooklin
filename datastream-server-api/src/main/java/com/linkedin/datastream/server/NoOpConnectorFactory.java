/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.server;

import java.util.List;
import java.util.Properties;

import com.linkedin.datastream.common.Datastream;
import com.linkedin.datastream.server.api.connector.Connector;
import com.linkedin.datastream.server.api.connector.ConnectorFactory;
import com.linkedin.datastream.server.api.connector.DatastreamValidationException;
import com.linkedin.datastream.server.providers.CheckpointProvider;


/**
 * NoOp connector that doesn't perform anything.
 */
public class NoOpConnectorFactory implements ConnectorFactory<NoOpConnectorFactory.NoOpConnector> {
  @Override
  public NoOpConnector createConnector(String connectorName, Properties config, String clusterName) {
    return new NoOpConnector();
  }

  public static class NoOpConnector implements Connector {
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
    }
  }
}
