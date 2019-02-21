package com.linkedin.datastream.kafka;

import java.util.Properties;

import com.linkedin.datastream.server.api.transport.TransportProviderAdmin;
import com.linkedin.datastream.server.api.transport.TransportProviderAdminFactory;


/**
 * Simple Kafka Transport provider factory that creates one producer for the entire system
 */
public class KafkaTransportProviderAdminFactory implements TransportProviderAdminFactory {

  @Override
  public TransportProviderAdmin createTransportProviderAdmin(String transportProviderName,
      Properties transportProviderProperties) {
    return new KafkaTransportProviderAdmin(transportProviderProperties);
  }
}
