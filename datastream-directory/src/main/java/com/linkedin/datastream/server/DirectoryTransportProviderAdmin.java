/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.server;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;

import com.linkedin.datastream.common.Datastream;
import com.linkedin.datastream.common.DatastreamDestination;
import com.linkedin.datastream.common.DatastreamRuntimeException;
import com.linkedin.datastream.server.api.connector.DatastreamValidationException;
import com.linkedin.datastream.server.api.transport.TransportProvider;
import com.linkedin.datastream.server.api.transport.TransportProviderAdmin;


/**
 * A {@link TransportProviderAdmin} implementation for {@link DirectoryTransportProvider}.
 */
public class DirectoryTransportProviderAdmin implements TransportProviderAdmin {
  private static final DirectoryTransportProvider DIRECTORY_TRANSPORT_PROVIDER =
      new DirectoryTransportProvider();

  private static final Duration DATASTREAM_RETENTION = Duration.ofDays(1);

  @Override
  public TransportProvider assignTransportProvider(DatastreamTask task) {
    return DIRECTORY_TRANSPORT_PROVIDER;
  }

  @Override
  public void unassignTransportProvider(DatastreamTask task) {
  }

  @Override
  public void initializeDestinationForDatastream(Datastream datastream, String destinationName)
      throws DatastreamValidationException {
    Validate.notNull(datastream);
    Validate.notEmpty(destinationName);

    if (!datastream.hasDestination()) {
      throw new DatastreamValidationException(String.format("Datastream '%s' must have a destination set", datastream));
    }

    DatastreamDestination destination = datastream.getDestination();
    if (StringUtils.isEmpty(destination.getConnectionString())) {
      throw new DatastreamValidationException("Datastream '%s' cannot have a null or empty connection string");
    }
  }

  @Override
  public void createDestination(Datastream datastream) {
    Validate.notNull(datastream);

    String destinationDirectory = datastream.getDestination().getConnectionString();
    try {
      Files.createDirectories(Paths.get(destinationDirectory));
    } catch (IOException e) {
      throw new DatastreamRuntimeException(
          String.format("Failed to create directory '%s' for datastream %s", destinationDirectory, datastream));
    }
  }

  @Override
  public void dropDestination(Datastream datastream) {
  }

  @Override
  public Duration getRetention(Datastream datastream) {
    return DATASTREAM_RETENTION;
  }
}
