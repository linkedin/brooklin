package com.linkedin.datastream.server;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.log4j.Logger;

import com.linkedin.datastream.metrics.BrooklinMetric;
import com.linkedin.datastream.server.api.transport.SendCallback;
import com.linkedin.datastream.server.api.transport.TransportException;
import com.linkedin.datastream.server.api.transport.TransportProvider;


public class InMemoryTransportProvider implements TransportProvider {
  private static final Logger LOG = Logger.getLogger(InMemoryTransportProvider.class);

  private HashMap<String, Integer> _topics = new HashMap<>();

  // Map of destination connection string to the list of events.
  private Map<String, List<DatastreamProducerRecord>> _recordsReceived = new HashMap<>();

  @Override
  public synchronized void createTopic(String topicName, int numberOfPartitions, Properties topicConfig)
      throws TransportException {
    if (_topics.containsKey(topicName)) {
      String msg = String.format("Topic %s already exists", topicName);
      LOG.error(msg);
      throw new TransportException(msg);
    }
    _topics.put(topicName, numberOfPartitions);
  }

  @Override
  public synchronized void dropTopic(String destination) throws TransportException {
    String topicName = getTopicName(destination);
    if (_topics.remove(topicName) == null) {
      String msg = String.format("Topic %s doesn't exist", topicName);
      LOG.error(msg);
      throw new TransportException(msg);
    }
  }

  public String getDestination(String topicName) {
    return topicName;
  }

  public static String getTopicName(String destination) {
    return destination;
  }

  @Override
  public synchronized void send(String connectionString, DatastreamProducerRecord record, SendCallback onComplete)
      throws TransportException {
    String topicName = getTopicName(connectionString);
    if (!_topics.containsKey(topicName)) {
      String msg = String.format("Topic %s doesn't exist", topicName);
      LOG.error(msg);
      throw new TransportException(msg);
    }

    if (!_recordsReceived.containsKey(connectionString)) {
      _recordsReceived.put(connectionString, new ArrayList<>());
    }

    LOG.info(String.format("Adding record with %d events to topic %s", record.getEvents().size(), topicName));

    _recordsReceived.get(connectionString).add(record);
  }

  @Override
  public void close() throws TransportException {
  }

  @Override
  public void flush() throws TransportException {
  }

  @Override
  public Duration getRetention(String destination) {
    return Duration.ofDays(1);
  }

  public Map<String, List<DatastreamProducerRecord>> getRecordsReceived() {
    return _recordsReceived;
  }

  @Override
  public List<BrooklinMetric> getMetrics() {
    return null;
  }

  public long getTotalEventsReceived(String connectionString) {
    long totalEventsReceived = _recordsReceived.getOrDefault(connectionString, Collections.emptyList())
        .stream()
        .map(DatastreamProducerRecord::getEvents)
        .flatMap(Collection::stream)
        .count();
    LOG.info(
        String.format("Total events received for the destination %s is %d", connectionString, totalEventsReceived));
    return totalEventsReceived;
  }
}
