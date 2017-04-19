package com.linkedin.datastream.connectors.oracle.triggerbased.consumer;

import java.util.Properties;

import com.linkedin.datastream.common.VerifiableProperties;


/**
 * OracleConsumerConfig is a config class that holds information about the
 * Oracle Database and any additional configs intended for the consumer logic
 *
 * The idea is to have all the credential information of the Oracle Databases
 * stored as configs, namespaced under their DB name.
 */
public class OracleConsumerConfig {
  private static final String DB_URI = "dbUri";

  private static final String DATA_SOURCE_FACTORY_CLASS = "dataSourceFactoryClass";
  private static final String DEFAULT_DATA_SOURCE_FACTORY_CLASS
      = "com.linkedin.datastream.connectors.oracle.triggerbased.consumer.DynamicDataSourceFactoryImpl";

  private static final String QUERY_HINT = "queryHint";
  private static final String DEFAULT_EVENT_QUERY_HINTS = "/*+ first_rows LEADING(tx) +*/";

  private static final String QUERY_TIMEOUT = "queryTimeout";
  private static final int DEFAULT_QUERY_TIMEOUT = -1;

  // If the resultSet is 1000 rows, with fetchSize set to 100, it would take 10 network
  // calls to process the entire resultSet. The default fetchSize is 10, which is way to small
  // since we expect to be seeing at least 100 rows per query in a Production Environment
  private static final String FETCH_SIZE = "fetchSize";
  private static final int DEFAULT_FETCH_SIZE = 100;

  private final String _queryHint;
  private final String _dataSourceFactoryClass;

  private final int _fetchSize;
  private final int _queryTimeout;

  private final String _dbUri;

  public OracleConsumerConfig(Properties properties) {
    VerifiableProperties verifiableProperties = new VerifiableProperties(properties);

    _dataSourceFactoryClass = verifiableProperties.getString(DATA_SOURCE_FACTORY_CLASS, DEFAULT_DATA_SOURCE_FACTORY_CLASS);
    _queryHint = verifiableProperties.getString(QUERY_HINT, DEFAULT_EVENT_QUERY_HINTS);

    _fetchSize = verifiableProperties.getInt(FETCH_SIZE, DEFAULT_FETCH_SIZE);

    _dbUri = verifiableProperties.getString(DB_URI);

    _queryTimeout = verifiableProperties.getInt(QUERY_TIMEOUT, DEFAULT_QUERY_TIMEOUT);

    verifiableProperties.verify();
  }

  public String getQueryHint() {
    return _queryHint;
  }

  public String getDataSourceFactoryClass() {
    return _dataSourceFactoryClass;
  }

  public int getFetchSize() {
    return _fetchSize;
  }

  public String getDbUri() {
    return _dbUri;
  }

  public int getQueryTimeout() {
    return _queryTimeout;
  }
}