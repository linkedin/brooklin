package com.linkedin.datastream.common;

import javax.sql.DataSource;

/*
  TODO: ckung remove after usage of this in other MP(s) uses the version moved to /databases subdirectory
 */
public interface OracleDataSourceFactory {
  DataSource createOracleDataSource(String uri, int queryTimeout) throws Exception;
}
