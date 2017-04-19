package com.linkedin.datastream.connectors.oracle.triggerbased.consumer;

import javax.sql.DataSource;

public interface OracleDataSourceFactory {
  DataSource createOracleDataSource(String uri, int queryTimeout) throws Exception;
}
