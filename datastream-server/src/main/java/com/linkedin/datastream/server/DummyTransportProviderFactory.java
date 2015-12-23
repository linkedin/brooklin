package com.linkedin.datastream.server;

import java.util.Properties;

import com.linkedin.datastream.server.api.transport.TransportException;
import com.linkedin.datastream.server.api.transport.TransportProvider;
import com.linkedin.datastream.server.api.transport.TransportProviderFactory;


public class DummyTransportProviderFactory implements TransportProviderFactory {
  @Override
  public TransportProvider createTransportProvider(Properties config) {
    return new TransportProvider() {
      @Override
      public String createTopic(String topicName, int numberOfPartitions, Properties topicConfig) {
        return topicName;
      }

      @Override
      public void dropTopic(String destination) {

      }

      @Override
      public void send(DatastreamEventRecord record) {

      }

      @Override
      public void close()
          throws TransportException {
      }

      @Override
      public void flush() {

      }
    };
  }
}
