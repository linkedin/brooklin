package com.linkedin.datastream.server;

import java.util.Properties;

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
      public void flush() {

      }
    };
  }
}
