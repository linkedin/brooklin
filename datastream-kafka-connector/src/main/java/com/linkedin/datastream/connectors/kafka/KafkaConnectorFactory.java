package com.linkedin.datastream.connectors.kafka;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Properties;

import com.linkedin.datastream.common.DatastreamRuntimeException;
import com.linkedin.datastream.common.ReflectionUtils;
import com.linkedin.datastream.common.VerifiableProperties;
import com.linkedin.datastream.server.api.connector.ConnectorFactory;


public class KafkaConnectorFactory implements ConnectorFactory<KafkaConnector> {

  public static final String DOMAIN_KAFKA_CONSUMER = "consumer";

  @Override
  public KafkaConnector createConnector(String connectorName, Properties config) {
    VerifiableProperties verifiableProperties = new VerifiableProperties(config);
    long commitMillis = verifiableProperties.getLongInRange(KafkaConnector.CONFIG_COMMIT_INTERVAL_MILLIS,
        Duration.ofMinutes(1).toMillis(), 0, Long.MAX_VALUE);

    String factory = verifiableProperties.getString(KafkaConnector.CONFIG_CONSUMER_FACTORY_CLASS,
        KafkaConsumerFactoryImpl.class.getName());
    KafkaConsumerFactory<?, ?> kafkaConsumerFactory = ReflectionUtils.createInstance(factory);
    if (kafkaConsumerFactory == null) {
      throw new DatastreamRuntimeException("Unable to instantiate factory class: " + factory);
    }

    List<KafkaBrokerAddress> brokers =
        Optional.ofNullable(verifiableProperties.getString(KafkaConnector.CONFIG_WHITE_LISTED_CLUSTERS, null))
            .map(KafkaConnectionString::parseBrokers)
            .orElse(Collections.emptyList());

    Properties kafkaConsumerProps = verifiableProperties.getDomainProperties(DOMAIN_KAFKA_CONSUMER);
    return new KafkaConnector(connectorName, commitMillis, kafkaConsumerFactory, kafkaConsumerProps, brokers);
  }
}
