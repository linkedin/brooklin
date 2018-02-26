package com.linkedin.datastream.common.databases.dbreader;

import java.util.Properties;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.datastream.common.DatastreamRuntimeException;
import com.linkedin.datastream.common.ReflectionUtils;
import com.linkedin.datastream.common.SqlTypeInterpreter;
import com.linkedin.datastream.common.VerifiableProperties;


/**
 * Configurations for the DatabaseChunkedReader class.
 */
public class DatabaseChunkedReaderConfig {
  private static final Logger LOG = LoggerFactory.getLogger(DatabaseChunkedReaderConfig.class);
  public static final String DB_READER_DOMAIN_CONFIG = "dbReader";
  public static final String QUERY_TIMEOUT_SECS = "queryTimeout";
  // If the resultSet is 1000 rows, with fetchSize set to 100, it would take 10 network
  // calls to process the entire resultSet. The default fetchSize is 10, which is way too small
  // for both a typical bootstrap scenario.
  public static final String FETCH_SIZE = "fetchSize";
  public static final String SKIP_BAD_MESSAGE = "skipBadMessage";
  // Number of rows to chunk on each query. This will limit the number of rows that the server will limit query result to
  public static final String CHUNK_SIZE = "chunk.size";
  public static final String NUM_CHUNK_BUCKETS = "chunk.numBuckets";
  public static final String CHUNK_INDEX = "chunk.index";
  public static final String DATABASE_INTERPRETER_CLASS_NAME = "database.reader";
  public static final String DATABASE_QUERY_MANAGER_CLASS_NAME = "database.queryManager";

  private static final int DEFAULT_QUERY_TIMEOUT_SECS = 0;
  private static final int DEFAULT_FETCH_SIZE = 100;
  private static final long DEFAULT_CHUNK_SIZE = 10000;
  private static final boolean DEFAULT_SKIP_BAD_MESSAGE = false;

  private final int _queryTimeout;
  private final int _fetchSize;
  private final long _chunkSize;
  private final long _numChunkBuckets;
  private final long _chunkIndex;
  private SqlTypeInterpreter _interpreter;
  private ChunkedQueryManager _chunkedQueryManager;
  private boolean _shouldSkipBadMessage;

  public DatabaseChunkedReaderConfig(Properties properties) {
    Properties props = new VerifiableProperties(properties).getDomainProperties(DB_READER_DOMAIN_CONFIG);
    VerifiableProperties verifiableProperties = new VerifiableProperties(props);
    _queryTimeout = verifiableProperties.getInt(QUERY_TIMEOUT_SECS, DEFAULT_QUERY_TIMEOUT_SECS);
    Validate.inclusiveBetween(0, Integer.MAX_VALUE, _queryTimeout);  // 0 being no limit.
    _fetchSize = verifiableProperties.getInt(FETCH_SIZE, DEFAULT_FETCH_SIZE);
    Validate.inclusiveBetween(0, Integer.MAX_VALUE, _fetchSize);
    _chunkSize = verifiableProperties.getLong(CHUNK_SIZE, DEFAULT_CHUNK_SIZE);
    Validate.inclusiveBetween(100, Long.MAX_VALUE, _fetchSize);
    _numChunkBuckets = verifiableProperties.getLong(NUM_CHUNK_BUCKETS);
    Validate.inclusiveBetween(0, Long.MAX_VALUE, _fetchSize);
    _chunkIndex = verifiableProperties.getLong(CHUNK_INDEX);
    Validate.inclusiveBetween(0, Long.MAX_VALUE, _fetchSize);
    _shouldSkipBadMessage = verifiableProperties.getBoolean(SKIP_BAD_MESSAGE, DEFAULT_SKIP_BAD_MESSAGE);

    String tableReader = verifiableProperties.getString(DATABASE_INTERPRETER_CLASS_NAME);
    if (StringUtils.isBlank(tableReader)) {
      String msg = "Database type interpreter class name is not set or is blank";
      LOG.error(msg);
      throw new DatastreamRuntimeException(msg);
    }
    _interpreter = ReflectionUtils.createInstance(tableReader);

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

  public long getChunkSize() {
    return _chunkSize;
  }

  public long getNumChunkBuckets() {
    return _numChunkBuckets;
  }

  public long getChunkIndex() {
    return _chunkIndex;
  }

  public SqlTypeInterpreter getDatabaseInterpreter() {
    return _interpreter;
  }

  public ChunkedQueryManager getChunkedQueryManager() {
    return _chunkedQueryManager;
  }

  public boolean shouldSkipBadMessage() {
    return _shouldSkipBadMessage;
  }
}
