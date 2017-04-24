package com.linkedin.datastream.connectors.oracle.triggerbased;

import com.linkedin.datastream.connectors.oracle.triggerbased.consumer.OracleConsumerConfig;
import java.util.Properties;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.linkedin.datastream.common.DatastreamException;
import com.linkedin.datastream.common.VerifiableProperties;


public class OracleConnectorConfig {
  static final String DAEMON_THREAD_INTERVAL_SECONDS = "daemonThreadIntervalInSeconds";
  private static final int DEFAULT_DAEMON_THREAD_INTERVAL_SECONDS = 300;

  private static final String ORACLE_CONSUMER_CONFIG = "oracleConsumer.%s";
  private static final String SCHEMA_REGISTRY_CONFIG = "schemaRegistry.%s";
  private static final String SCHEMA_NAME = "schemaRegistryName";

  private static final Map<String, OracleConsumerConfig> CONSUMER_CONFIG_CACHE  = new ConcurrentHashMap<>();
  private static final Map<String, Properties> SCHEMA_CONFIG_CACHE = new ConcurrentHashMap<>();

  private final VerifiableProperties _verifiableProperties;

  // How often the DatastreamTaskManager checks to see ensure the EventReaders are running
  private final int _daemonThreadIntervalSeconds;

  // It possible for to provide different types of SchemaRegistries. Therefore we need a name for the schema Registry
  // In other words, you need 2 configs
  // 1) schemaRegistryName = "confluentSchemaRegistry"
  // 2.1) schemaRegistry.confluentSchemaRegistry.mode = "mode"
  // 2.2) schemaRegistry.confluentSchemaRegistry.uri = "uri"
  private final String _schemaRegistryName;

  public OracleConnectorConfig(Properties properties) throws DatastreamException {
    _verifiableProperties = new VerifiableProperties(properties);

    _daemonThreadIntervalSeconds = _verifiableProperties.getInt(DAEMON_THREAD_INTERVAL_SECONDS, DEFAULT_DAEMON_THREAD_INTERVAL_SECONDS);
    _schemaRegistryName = _verifiableProperties.getString(SCHEMA_NAME);

    _verifiableProperties.verify();
  }

  public Properties getSchemaRegistryConfig() {
    if (SCHEMA_CONFIG_CACHE.containsKey(_schemaRegistryName)) {
      return SCHEMA_CONFIG_CACHE.get(_schemaRegistryName);
    }

    Properties schemaConfig = _verifiableProperties.getDomainProperties(String.format(SCHEMA_REGISTRY_CONFIG, _schemaRegistryName));

    SCHEMA_CONFIG_CACHE.put(_schemaRegistryName, schemaConfig);
    return schemaConfig;
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
