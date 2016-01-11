package com.linkedin.datastream.kafka;

import java.util.Properties;

import org.apache.commons.lang.Validate;

import com.linkedin.datastream.common.VerifiableProperties;
import com.linkedin.datastream.server.api.transport.TransportProvider;
import com.linkedin.datastream.server.api.transport.TransportProviderFactory;


/**
 * Factory that creates a KafkaTransportProvider
 */
public class KafkaTransportProviderFactory implements TransportProviderFactory {

  public static final String KAFKA_CONFIG_PREFIX = "kafka";

  @Override
  public TransportProvider createTransportProvider(Properties config) {
    Validate.notNull(config, "null config");
    VerifiableProperties kafkaProps = new VerifiableProperties(config);
    kafkaProps.getDomainProperties(KAFKA_CONFIG_PREFIX);
    return new KafkaTransportProvider(config);
  }
}
