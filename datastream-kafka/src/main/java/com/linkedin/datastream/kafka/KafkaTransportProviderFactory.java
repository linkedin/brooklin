package com.linkedin.datastream.kafka;

import com.linkedin.datastream.common.VerifiableProperties;
import com.linkedin.datastream.server.TransportProvider;
import com.linkedin.datastream.server.TransportProviderFactory;

import java.util.Objects;
import java.util.Properties;


/**
 * Factory that creates a KafkaTransportProvider
 */
public class KafkaTransportProviderFactory implements TransportProviderFactory {
  private static final String CONFIG_PREFIX = "datastream.server.transportProvider.";

  @Override
  public TransportProvider createTransportProvider(Properties config) {
    Objects.requireNonNull(config, "null config");
    VerifiableProperties props = new VerifiableProperties(config);
    return new KafkaTransportProvider(props.getDomainProperties(CONFIG_PREFIX));
  }
}
