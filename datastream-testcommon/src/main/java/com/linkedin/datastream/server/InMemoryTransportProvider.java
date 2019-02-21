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


public class InMemoryTransportProvider implements TransportProvider {
  private static final Logger LOG = LoggerFactory.getLogger(InMemoryTransportProvider.class);

  private HashMap<String, Integer> _topics = new HashMap<>();

  // Map of destination connection string to the list of events.
  private Map<String, List<DatastreamProducerRecord>> _recordsReceived = new ConcurrentHashMap<>();

  public static String getTopicName(String destination) {
    return destination;
  }

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

  public Map<String, List<DatastreamProducerRecord>> getRecordsReceived() {
    return _recordsReceived;
  }

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

  public void addTopic(String topicName, int numberOfPartitions) {
    _topics.put(topicName, numberOfPartitions);
  }

  public void removeTopic(String topicName) {
    _topics.remove(topicName);
  }
}
