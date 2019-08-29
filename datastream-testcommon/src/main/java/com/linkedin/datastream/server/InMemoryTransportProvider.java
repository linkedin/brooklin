/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.server;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.datastream.common.DatastreamRuntimeException;
import com.linkedin.datastream.server.api.transport.SendCallback;
import com.linkedin.datastream.server.api.transport.TransportProvider;


/**
 * An in-memory implementation of {@link TransportProvider} that caches all the events passed to its
 * {@link InMemoryTransportProvider#send(String, DatastreamProducerRecord, SendCallback)} method
 */
public class InMemoryTransportProvider implements TransportProvider {
  private static final Logger LOG = LoggerFactory.getLogger(InMemoryTransportProvider.class);

  private final HashMap<String, Integer> _topics = new HashMap<>();

  // Map of destination connection string to the list of events.
  private final Map<String, List<DatastreamProducerRecord>> _recordsReceived = new ConcurrentHashMap<>();

  /**
   * Get the topic name corresponding to the provided destination
   */
  public static String getTopicName(String destination) {
    return destination;
  }

  /**
   * Get all the topics added to this transport provider, along with their partition counts
   */
  public HashMap<String, Integer> getTopics() {
    return _topics;
  }

  @Override
  public synchronized void send(String connectionString, DatastreamProducerRecord record, SendCallback onComplete) {
    String topicName = getTopicName(connectionString);
    if (!_topics.containsKey(topicName)) {
      String msg = String.format("Topic %s doesn't exist", topicName);
      LOG.error(msg);
      throw new DatastreamRuntimeException(msg);
    }

    if (!_recordsReceived.containsKey(connectionString)) {
      _recordsReceived.put(connectionString, new ArrayList<>());
    }

    LOG.info("Adding record with {} events to topic {}", record.getEvents().size(), topicName);

    _recordsReceived.get(connectionString).add(record);
  }

  @Override
  public void close() {
  }

  @Override
  public void flush() {
  }

  /**
   * Get all records passed to {@link InMemoryTransportProvider#send(String, DatastreamProducerRecord, SendCallback)}
   * so far
   */
  public Map<String, List<DatastreamProducerRecord>> getRecordsReceived() {
    return _recordsReceived;
  }

  /**
   * Get the number of events passed to {@link InMemoryTransportProvider#send(String, DatastreamProducerRecord, SendCallback)}
   * so far, with the specified {@code connectionString}
   */
  public long getTotalEventsReceived(String connectionString) {
    if (!_recordsReceived.containsKey(connectionString)) {
      return 0L;
    }

    long totalEventsReceived =
        _recordsReceived.get(connectionString).stream().mapToInt(r -> r.getEvents().size()).sum();
    LOG.info(
        String.format("Total events received for the destination %s is %d", connectionString, totalEventsReceived));
    return totalEventsReceived;
  }

  /**
   * Add the specified topic and its corresponding partition count to the list of topics recognized by this transport
   * provider
   */
  public void addTopic(String topicName, int numberOfPartitions) {
    _topics.put(topicName, numberOfPartitions);
  }

  /**
   * Remove a previously added topic from the set of topics recognized by this transport provider
   */
  public void removeTopic(String topicName) {
    _topics.remove(topicName);
  }
}
