package com.linkedin.datastream.common.databases;

import javax.sql.DataSource;


public interface OracleDataSourceFactory {
  DataSource createOracleDataSource(String uri, int queryTimeout) throws Exception;
}
