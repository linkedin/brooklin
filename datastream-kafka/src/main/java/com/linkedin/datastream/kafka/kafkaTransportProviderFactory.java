package com.linkedin.datastream.kafka;

import java.util.Properties;

import com.linkedin.datastream.server.TransportProvider;
import com.linkedin.datastream.server.TransportProviderFactory;


/**
 * Factory that creates a KafkaTransportProvider
 */
public class kafkaTransportProviderFactory implements TransportProviderFactory {
  @Override
  public TransportProvider createTransportProvider(Properties transportProviderProperties) {
    return new KafkaTransportProvider(transportProviderProperties);
  }
}
