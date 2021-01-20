/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.server;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.datastream.common.Datastream;
import com.linkedin.datastream.common.DatastreamDestination;
import com.linkedin.datastream.common.DatastreamRuntimeException;
import com.linkedin.datastream.server.api.transport.TransportProvider;
import com.linkedin.datastream.server.api.transport.TransportProviderAdmin;


/**
 * A {@link TransportProviderAdmin} implementation for {@link InMemoryTransportProvider}.
 */
public class InMemoryTransportProviderAdmin implements TransportProviderAdmin {

  private static final Logger LOG = LoggerFactory.getLogger(InMemoryTransportProviderAdminFactory.class);
  private static final int DEFAULT_NUMBER_PARTITIONS = 1;
  private static final InMemoryTransportProvider TRANSPORT_PROVIDER = new InMemoryTransportProvider();

  /**
   * Get InMemoryTransportProvider singleton
   */
  public static InMemoryTransportProvider getTransportProvider() {
    return TRANSPORT_PROVIDER;
  }

  @Override
  public Duration getRetention(Datastream datastream) {
    return Duration.ofDays(1);
  }

  @Override
  public synchronized void createDestination(Datastream datastream) {
    String topicName = datastream.getDestination().getConnectionString();
    int numberOfPartitions = datastream.getDestination().getPartitions();
    if (TRANSPORT_PROVIDER.getTopics().containsKey(topicName)) {
      LOG.warn("Topic {} already exists", topicName);
    }
    TRANSPORT_PROVIDER.addTopic(topicName, numberOfPartitions);
  }

  @Override
  public synchronized void dropDestination(Datastream datastream) {

    String topicName = datastream.getDestination().getConnectionString();
    if (!TRANSPORT_PROVIDER.getTopics().containsKey(topicName)) {
      String msg = String.format("Topic %s doesn't exist", topicName);
      LOG.error(msg);
      throw new DatastreamRuntimeException(msg);
    }
  }

  @Override
  public TransportProvider assignTransportProvider(DatastreamTask task) {
    return TRANSPORT_PROVIDER;
  }

  @Override
  public void unassignTransportProvider(DatastreamTask task) {
  }

  @Override
  public void unassignTransportProvider(List<DatastreamTask> taskList) {
  }

  @Override
  public void initializeDestinationForDatastream(Datastream datastream, String destinationName) {

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
