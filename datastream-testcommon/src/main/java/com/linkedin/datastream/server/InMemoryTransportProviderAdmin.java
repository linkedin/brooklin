package com.linkedin.datastream.server;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

import org.apache.log4j.Logger;

import com.linkedin.datastream.common.Datastream;
import com.linkedin.datastream.common.DatastreamDestination;
import com.linkedin.datastream.common.DatastreamRuntimeException;
import com.linkedin.datastream.server.api.transport.TransportProvider;
import com.linkedin.datastream.server.api.transport.TransportProviderAdmin;


public class InMemoryTransportProviderAdmin implements TransportProviderAdmin {

  public InMemoryTransportProviderAdmin() {

  }

  private static final Logger LOG = Logger.getLogger(InMemoryTransportProviderAdminFactory.class);
  private static final int DEFAULT_NUMBER_PARTITIONS = 1;

  private static InMemoryTransportProvider _transportProvider = new InMemoryTransportProvider();

  public static InMemoryTransportProvider getTransportProvider() {
    return _transportProvider;
  }

  @Override
  public Duration getRetention(Datastream datastream) {
    return Duration.ofDays(1);
  }

  public synchronized void createDestination(Datastream datastream) {
    String topicName = datastream.getDestination().getConnectionString();
    int numberOfPartitions = datastream.getDestination().getPartitions();
    if (_transportProvider.getTopics().containsKey(topicName)) {
      String msg = String.format("Topic %s already exists", topicName);
      LOG.warn(msg);
    }
    _transportProvider.addTopic(topicName, numberOfPartitions);
  }

  @Override
  public synchronized void dropDestination(Datastream datastream) {

    String topicName = datastream.getDestination().getConnectionString();
    if (!_transportProvider.getTopics().containsKey(topicName)) {
      String msg = String.format("Topic %s doesn't exist", topicName);
      LOG.error(msg);
      throw new DatastreamRuntimeException(msg);
    }
  }

  @Override
  public TransportProvider assignTransportProvider(DatastreamTask task) {
    return _transportProvider;
  }

  @Override
  public void unassignTransportProvider(DatastreamTask task) {
  }

  @Override
  public void initializeDestinationForDatastream(Datastream datastream) {

    if (!datastream.hasDestination()) {
      datastream.setDestination(new DatastreamDestination());
    }

    DatastreamDestination destination = datastream.getDestination();
    if (!destination.hasConnectionString() || destination.getConnectionString().isEmpty()) {
      destination.setConnectionString(getTopicName(datastream.getName()));
    }

    if (!destination.hasPartitions() || destination.getPartitions() <= 0) {
      destination.setPartitions(DEFAULT_NUMBER_PARTITIONS);
    }
  }

  private String getTopicName(String datastreamName) {
    DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMddHHmmss");
    String currentTime = formatter.format(LocalDateTime.now());
    return String.format("%s_%s", datastreamName, currentTime);
  }
}
