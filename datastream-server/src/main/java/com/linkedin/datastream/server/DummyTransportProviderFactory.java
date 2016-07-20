package com.linkedin.datastream.server;

import java.time.Duration;
import java.util.Map;
import java.util.Properties;

import com.codahale.metrics.Metric;

import com.linkedin.datastream.common.DatastreamRuntimeException;
import com.linkedin.datastream.server.api.transport.DatastreamRecordMetadata;
import com.linkedin.datastream.server.api.transport.SendCallback;
import com.linkedin.datastream.server.api.transport.TransportException;
import com.linkedin.datastream.server.api.transport.TransportProvider;
import com.linkedin.datastream.server.api.transport.TransportProviderFactory;


public class DummyTransportProviderFactory implements TransportProviderFactory {

  private final boolean _throwOnSend;

  public DummyTransportProviderFactory() {
    this(false);
  }

  public DummyTransportProviderFactory(boolean throwOnSend) {
    _throwOnSend = throwOnSend;
  }

  @Override
  public TransportProvider createTransportProvider(Properties config) {
    return new TransportProvider() {
      @Override
      public String getDestination(String topicName) {
        return topicName;
      }

      @Override
      public void createTopic(String topicName, int numberOfPartitions, Properties topicConfig) {
      }

      @Override
      public void dropTopic(String destination) {

      }

      @Override
      public void send(String destination, DatastreamProducerRecord record, SendCallback onComplete)
          throws TransportException {
        if (_throwOnSend && onComplete != null) {
          onComplete.onCompletion(
              new DatastreamRecordMetadata(record.getCheckpoint(), destination, record.getPartition().get()),
              new DatastreamRuntimeException());
        }
      }

      @Override
      public void close() throws TransportException {
      }

      @Override
      public void flush() {

      }

      @Override
      public Duration getRetention(String destination) {
        return Duration.ofDays(3);
      }

      @Override
      public Map<String, Metric> getMetrics() {
        return null;
      }
    };
  }
}
