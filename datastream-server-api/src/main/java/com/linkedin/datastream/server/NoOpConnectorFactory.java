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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    private static final Logger LOG = LoggerFactory.getLogger(NoOpConnector.class.getName());
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
      return ((!newDatastream.hasSource() && !existingDatastream.hasSource())
          || (newDatastream.hasSource() && existingDatastream.hasSource()
          && newDatastream.getSource().equals(existingDatastream.getSource())));
    }

    private boolean doesDestinationMatch(Datastream newDatastream, Datastream existingDatastream) {
      return ((!newDatastream.hasDestination() && !existingDatastream.hasDestination())
          || (newDatastream.hasDestination() && existingDatastream.hasDestination()
          && newDatastream.getDestination().equals(existingDatastream.getDestination())));
    }

    /**
     * Throws {@link DatastreamValidationException} if datastream update is not allowed.
     * Validates that the source and destination are not being updated. Only metadata can be updated.
     * @param datastreams list of datastreams to be updated
     * @param allDatastreams all existing datastreams in the system of connector type of the datastream that is being
     *                       validated.
     * @throws DatastreamValidationException
     */
    @Override
    public void validateUpdateDatastreams(List<Datastream> datastreams, List<Datastream> allDatastreams)
      throws DatastreamValidationException {
      for (Datastream newDatastream : datastreams) {
        List<Datastream> existingDatastreams = allDatastreams.stream().filter(ds -> ds.getName()
            .equals(newDatastream.getName())).collect(Collectors.toList());

        Validate.isTrue(existingDatastreams.size() == 1, "Bad state: Multiple datastreams exist with the "
            + "same name " + existingDatastreams);
        Datastream existingDatastream = existingDatastreams.get(0);

        if (newDatastream.equals(existingDatastream)) {
          LOG.info(String.format("Skipping update for datastream {%s} due to no change", existingDatastream));
          continue;
        }

        if (!doesSourceMatch(newDatastream, existingDatastream)
            || !doesDestinationMatch(newDatastream, existingDatastream)) {
          throw new DatastreamValidationException(String.format("Only metadata update is allowed. "
                  + "Source and destination cannot be updated. Old stream: {%s}; New stream: {%s}", existingDatastream,
              newDatastream));
        }

        // Only metadata updates are allowed
        if (newDatastream.hasMetadata() && existingDatastream.hasMetadata()
            && newDatastream.getMetadata().equals(existingDatastream.getMetadata())) {
          throw new DatastreamValidationException(String.format("Only metadata update is allowed. "
              + "Old stream: {%s}; New stream: {%s}", existingDatastream, newDatastream));
        }
      }
    }
  }
}
