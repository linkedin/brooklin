/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.server;

import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

import org.apache.commons.lang3.Validate;

import com.linkedin.datastream.common.Datastream;
import com.linkedin.datastream.server.api.connector.Connector;
import com.linkedin.datastream.server.api.connector.ConnectorFactory;
import com.linkedin.datastream.server.api.connector.DatastreamValidationException;
import com.linkedin.datastream.server.providers.CheckpointProvider;


/**
 * A factory for creating {@link NoOpConnector} instances
 */
public class NoOpConnectorFactory implements ConnectorFactory<NoOpConnectorFactory.NoOpConnector> {
  @Override
  public NoOpConnector createConnector(String connectorName, Properties config, String clusterName) {
    return new NoOpConnector();
  }

  /**
   * A {@link Connector} implementation that does nothing.
   */
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

    private boolean doesSourceMatch(Datastream newDatastream, Datastream existingDatastream) {
      if ((!newDatastream.hasSource() && !existingDatastream.hasSource())
          || (newDatastream.hasSource() && existingDatastream.hasSource()
          && newDatastream.getSource().equals(existingDatastream.getSource()))) {
        return true;
      }
      return false;
    }

    private boolean doesDestinationMatch(Datastream newDatastream, Datastream existingDatastream) {
      if ((!newDatastream.hasDestination() && !existingDatastream.hasDestination())
          || (newDatastream.hasDestination() && existingDatastream.hasDestination()
          && newDatastream.getDestination().equals(existingDatastream.getDestination()))) {
        return true;
      }
      return false;
    }

    @Override
    public void validateUpdateDatastreams(List<Datastream> datastreams, List<Datastream> allDatastreams)
      throws DatastreamValidationException {
      // Only metadata updates are allowed
      for (Datastream newDatastream : datastreams) {
        List<Datastream> existingDatastreams = allDatastreams.stream().filter(ds -> ds.getName()
            .equals(newDatastream.getName())).collect(Collectors.toList());

        Validate.isTrue(existingDatastreams.size() == 1, "Bad state: Multiple datastreams exist with the "
            + "same name " + existingDatastreams);
        Datastream existingDatastream = existingDatastreams.get(0);

        if (newDatastream.equals(existingDatastream)) {
          continue;
        }

        if (!doesSourceMatch(newDatastream, existingDatastream)
            || !doesDestinationMatch(newDatastream, existingDatastream)) {
          throw new DatastreamValidationException(String.format("Only metadata update is allowed. "
                  + "Source and destination cannot be updated. Old stream: {%s}; New stream: {%s}", existingDatastream,
              newDatastream));
        }

        if (newDatastream.hasMetadata() && existingDatastream.hasMetadata()
            && newDatastream.getMetadata().equals(existingDatastream.getMetadata())) {
          throw new DatastreamValidationException(String.format("Only metadata update is allowed. "
              + "Old stream: {%s}; New stream: {%s}", existingDatastream, newDatastream));
        }
      }
    }
  }
}
