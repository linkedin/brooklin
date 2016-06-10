package com.linkedin.datastream.server;

import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.log4j.Logger;

import com.codahale.metrics.Metric;

import com.linkedin.datastream.server.api.transport.SendCallback;
import com.linkedin.datastream.server.api.transport.TransportException;
import com.linkedin.datastream.server.api.transport.TransportProvider;


public class InMemoryTransportProvider implements TransportProvider {
  private static final Logger LOG = Logger.getLogger(InMemoryTransportProvider.class);

  private Map<String, Metric> _metrics = new HashMap<>();

  private HashMap<String, Integer> _topics = new HashMap<>();

  private Map<String, List<DatastreamProducerRecord>> _recordsReceived = new HashMap<>();

  public static final String URI_FORMAT = "memory://%s";

  @Override
  public synchronized String createTopic(String topicName, int numberOfPartitions, Properties topicConfig)
      throws TransportException {
    if (_topics.containsKey(topicName)) {
      String msg = String.format("Topic %s already exists", topicName);
      LOG.error(msg);
      throw new TransportException(msg);
    }
    _topics.put(topicName, numberOfPartitions);
    return String.format(URI_FORMAT, topicName);
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

  public static String getDestination(String topicName) {
    return String.format(URI_FORMAT, topicName);
  }

  public static String getTopicName(String destination) {
    return URI.create(destination).getPath().substring(1);
  }

  @Override
  public synchronized void send(String destination, DatastreamProducerRecord record, SendCallback onComplete)
      throws TransportException {
    String topicName = getTopicName(destination);
    if (!_topics.containsKey(topicName)) {
      String msg = String.format("Topic %s doesn't exist", topicName);
      LOG.error(msg);
      throw new TransportException(msg);
    }

    if (!_recordsReceived.containsKey(destination)) {
      _recordsReceived.put(destination, new ArrayList<>());
    }

    _recordsReceived.get(destination).add(record);
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
  public Map<String, Metric> getMetrics() {
    return _metrics;
  }
}
