/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.server;

import java.time.Duration;
import java.util.Properties;

import org.apache.commons.lang.NotImplementedException;

import com.linkedin.datastream.common.Datastream;
import com.linkedin.datastream.server.api.connector.DatastreamValidationException;
import com.linkedin.datastream.server.api.transport.DatastreamRecordMetadata;
import com.linkedin.datastream.server.api.transport.SendCallback;
import com.linkedin.datastream.server.api.transport.TransportProvider;
import com.linkedin.datastream.server.api.transport.TransportProviderAdmin;
import com.linkedin.datastream.server.api.transport.TransportProviderAdminFactory;


/**
 * Factory for the NoOpTransportProvider that doesn't perform anything.
 */
public class NoOpTransportProviderAdminFactory implements TransportProviderAdminFactory {
  @Override
  public TransportProviderAdmin createTransportProviderAdmin(String transportProviderName,
      Properties transportProviderProperties) {
    return new NoOpTransportProviderAdmin();
  }

  public static class NoOpTransportProvider implements TransportProvider {

    @Override
    public void send(String destination, DatastreamProducerRecord record, SendCallback onComplete) {
      DatastreamRecordMetadata metadata =  new DatastreamRecordMetadata(
          record.getCheckpoint(), null, record.getPartition().orElse(null));
      onComplete.onCompletion(metadata, null);
    }

    @Override
    public void close() {
    }

    @Override
    public void flush() {
    }
  }

  public static class NoOpTransportProviderAdmin implements TransportProviderAdmin {
    @Override
    public TransportProvider assignTransportProvider(DatastreamTask task) {
      return new NoOpTransportProvider();
    }

    @Override
    public void unassignTransportProvider(DatastreamTask task) {
    }

    @Override
    public void initializeDestinationForDatastream(Datastream datastream, String destinationName)
        throws DatastreamValidationException {
    }

    @Override
    public void createDestination(Datastream datastream) {
    }

    @Override
    public void dropDestination(Datastream datastream) {
    }

    @Override
    public Duration getRetention(Datastream datastream) {
      throw new NotImplementedException();
    }
  }
}
