package com.linkedin.datastream.server;

import java.time.Duration;
import java.util.Properties;

import com.linkedin.datastream.common.Datastream;
import com.linkedin.datastream.common.DatastreamDestination;
import com.linkedin.datastream.common.DatastreamRuntimeException;
import com.linkedin.datastream.server.api.connector.DatastreamValidationException;
import com.linkedin.datastream.server.api.transport.DatastreamRecordMetadata;
import com.linkedin.datastream.server.api.transport.SendCallback;
import com.linkedin.datastream.server.api.transport.TransportProvider;
import com.linkedin.datastream.server.api.transport.TransportProviderAdmin;
import com.linkedin.datastream.server.api.transport.TransportProviderAdminFactory;


public class DummyTransportProviderAdminFactory implements TransportProviderAdminFactory, TransportProviderAdmin {

  public static final String PROVIDER_NAME = "default";

  private final boolean _throwOnSend;

  public DummyTransportProviderAdminFactory() {
    this(false);
  }

  public DummyTransportProviderAdminFactory(boolean throwOnSend) {
    _throwOnSend = throwOnSend;
  }

  @Override
  public TransportProvider assignTransportProvider(DatastreamTask task) {
    return new TransportProvider() {

      @Override
      public void send(String destination, DatastreamProducerRecord record, SendCallback onComplete) {
        if (_throwOnSend && onComplete != null) {
          onComplete.onCompletion(
              new DatastreamRecordMetadata(record.getCheckpoint(), destination, record.getPartition().get()),
              new DatastreamRuntimeException());
        }
      }

      @Override
      public void close() {
      }

      @Override
      public void flush() {

      }
    };
  }

  @Override
  public void unassignTransportProvider(DatastreamTask task) {

  }

  @Override
  public void initializeDestinationForDatastream(Datastream datastream, String destinationName) throws DatastreamValidationException {

    if (!datastream.hasDestination()) {
      datastream.setDestination(new DatastreamDestination());
    }

    if (!datastream.getDestination().hasConnectionString()) {
      datastream.getDestination().setConnectionString(datastream.getName());
    }

    if (!datastream.getDestination().hasPartitions()) {
      datastream.getDestination().setPartitions(1);
    }
  }

  @Override
  public void createDestination(Datastream datastream) {

  }

  @Override
  public void dropDestination(Datastream datastream) {

  }

  @Override
  public Duration getRetention(Datastream datastream) {
    return Duration.ofDays(3);
  }

  @Override
  public TransportProviderAdmin createTransportProviderAdmin(String transportProviderName,
      Properties transportProviderProperties) {
    return this;
  }
}
