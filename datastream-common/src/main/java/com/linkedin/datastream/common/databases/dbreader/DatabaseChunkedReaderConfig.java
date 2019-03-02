/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.common.databases.dbreader;

import java.util.Properties;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.datastream.common.DatastreamRuntimeException;
import com.linkedin.datastream.common.ReflectionUtils;
import com.linkedin.datastream.common.VerifiableProperties;


/**
 * Configurations for the DatabaseChunkedReader class.
 */
public class DatabaseChunkedReaderConfig {
  public static final String DB_READER_DOMAIN_CONFIG = "dbReader";
  public static final String QUERY_TIMEOUT_SECS = "queryTimeout";
  // If the ResultSet is 10000 rows, with fetchSize set to 1000, it would take 10 network calls to fetch the entire
  // ResultSet from the server. This is used to limit the memory requirements on the client driver. Oracle server will
  // cache the query results and only return fetchSize rows to the driver.
  public static final String FETCH_SIZE = "fetchSize";
  public static final String SKIP_BAD_MESSAGE = "skipBadMessage";
  // Max number of rows to fetch for each query. This will help the server limit the number of full row
  // fetches that it has to do. For example in Oracle, a ROWNUM <= 1000 will add a stopKey constraint where the DB will
  // only look for first 1000 matches that match the specified constraints and will do a full row fetch only for these.
  public static final String ROW_COUNT_LIMIT = "chunk.rowCountLimit";
  public static final String DATABASE_QUERY_MANAGER_CLASS_NAME = "database.queryManager";
  private static final Logger LOG = LoggerFactory.getLogger(DatabaseChunkedReaderConfig.class);
  private static final int DEFAULT_QUERY_TIMEOUT_SECS = 0;
  private static final int DEFAULT_FETCH_SIZE = 10000;
  private static final long DEFAULT_ROW_COUNT_LIMIT = 50000;
  private static final boolean DEFAULT_SKIP_BAD_MESSAGE = false;

  private final int _queryTimeout;
  private final int _fetchSize;
  private final long _rowCountLimit;
  private ChunkedQueryManager _chunkedQueryManager;
  private boolean _shouldSkipBadMessage;

  public DatabaseChunkedReaderConfig(Properties properties) {
    Properties props = new VerifiableProperties(properties).getDomainProperties(DB_READER_DOMAIN_CONFIG);
    VerifiableProperties verifiableProperties = new VerifiableProperties(props);
    _queryTimeout = verifiableProperties.getInt(QUERY_TIMEOUT_SECS, DEFAULT_QUERY_TIMEOUT_SECS);
    Validate.inclusiveBetween(0, Integer.MAX_VALUE, _queryTimeout);  // 0 being no limit.
    _fetchSize = verifiableProperties.getInt(FETCH_SIZE, DEFAULT_FETCH_SIZE);
    Validate.inclusiveBetween(0, Integer.MAX_VALUE, _fetchSize);
    _rowCountLimit = verifiableProperties.getLong(ROW_COUNT_LIMIT, DEFAULT_ROW_COUNT_LIMIT);
    Validate.inclusiveBetween(100, Long.MAX_VALUE, _fetchSize);
    Validate.inclusiveBetween(0, Long.MAX_VALUE, _fetchSize);
    Validate.inclusiveBetween(0, Long.MAX_VALUE, _fetchSize);
    _shouldSkipBadMessage = verifiableProperties.getBoolean(SKIP_BAD_MESSAGE, DEFAULT_SKIP_BAD_MESSAGE);

    String queryManagerClass = verifiableProperties.getString(DATABASE_QUERY_MANAGER_CLASS_NAME);
    if (StringUtils.isBlank(queryManagerClass)) {
      String msg = "Database query manager class name is not set or is blank";
      LOG.error(msg);
      throw new DatastreamRuntimeException(msg);
    }
    _chunkedQueryManager = ReflectionUtils.createInstance(queryManagerClass);

    verifiableProperties.verify();
  }

  /**
   * Getters for private member fields
   */

  public int getFetchSize() {
    return _fetchSize;
  }

  public int getQueryTimeout() {
    return _queryTimeout;
  }

  public long getRowCountLimit() {
    return _rowCountLimit;
  }

  public ChunkedQueryManager getChunkedQueryManager() {
    return _chunkedQueryManager;
  }

  public boolean shouldSkipBadMessage() {
    return _shouldSkipBadMessage;
  }
}
