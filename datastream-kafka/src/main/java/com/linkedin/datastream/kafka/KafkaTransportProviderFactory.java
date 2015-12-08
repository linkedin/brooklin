package com.linkedin.datastream.kafka;

import java.util.Properties;

import org.apache.commons.lang.Validate;

import com.linkedin.datastream.common.VerifiableProperties;
import com.linkedin.datastream.server.TransportProvider;
import com.linkedin.datastream.server.TransportProviderFactory;


/**
 * Factory that creates a KafkaTransportProvider
 */
public class KafkaTransportProviderFactory implements TransportProviderFactory {
  private static final String CONFIG_PREFIX = "datastream.server.transportProvider.";

  @Override
  public TransportProvider createTransportProvider(Properties config) {
    Validate.notNull(config, "null config");
    VerifiableProperties props = new VerifiableProperties(config);
    return new KafkaTransportProvider(props.getDomainProperties(CONFIG_PREFIX));
  }
}
