package com.linkedin.datastream.connectors.oracle.triggerbased;

import com.linkedin.datastream.connectors.oracle.triggerbased.consumer.OracleConsumerConfig;
import java.util.Properties;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.linkedin.datastream.common.DatastreamException;
import com.linkedin.datastream.common.VerifiableProperties;


public class OracleConnectorConfig {
  private static final String DAEMON_THREAD_INTERVAL_SECONDS = "daemonThreadIntervalInSeconds";
  private static final int DEFAULT_DAEMON_THREAD_INTERVAL_SECONDS = 300;

  private static final String ORACLE_CONSUMER_CONFIG = "oracleConsumer.%s";
  private static final String KAFKA_SCHEMA_REGISTRY_CONFIG = "kafkaSchemaRegistry";

  private static final Map<String, OracleConsumerConfig> CONSUMER_CONFIG_CACHE  = new ConcurrentHashMap<>();

  private final VerifiableProperties _verifiableProperties;

  // How often the DatastreamTaskManager checks to see ensure the EventReaders are running
  private final int _daemonThreadIntervalSeconds;

  // configs for Kafka Schema Registry
  private final Properties _kafkaSchemaRegistryConfig;

  public OracleConnectorConfig(Properties properties) throws DatastreamException {
    _verifiableProperties = new VerifiableProperties(properties);

    _daemonThreadIntervalSeconds = _verifiableProperties.getInt(DAEMON_THREAD_INTERVAL_SECONDS, DEFAULT_DAEMON_THREAD_INTERVAL_SECONDS);

    _kafkaSchemaRegistryConfig = _verifiableProperties.getDomainProperties(KAFKA_SCHEMA_REGISTRY_CONFIG);

    _verifiableProperties.verify();
  }

  public Properties getLiKafkaSchemaRegistryConfig() {
    return _kafkaSchemaRegistryConfig;
  }

  public int getDaemonThreadIntervalSeconds() {
    return _daemonThreadIntervalSeconds;
  }

  public OracleConsumerConfig getOracleConsumerConfig(String dbName) {
    if (CONSUMER_CONFIG_CACHE.containsKey(dbName)) {
      return CONSUMER_CONFIG_CACHE.get(dbName);
    }

    OracleConsumerConfig config =
        new OracleConsumerConfig(_verifiableProperties.getDomainProperties(String.format(ORACLE_CONSUMER_CONFIG, dbName)));

    CONSUMER_CONFIG_CACHE.put(dbName, config);
    return config;
  }
}
