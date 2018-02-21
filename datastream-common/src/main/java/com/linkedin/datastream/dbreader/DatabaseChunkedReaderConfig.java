package com.linkedin.datastream.dbreader;

import java.util.Properties;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.datastream.common.SqlTypeInterpreter;
import com.linkedin.datastream.common.DatastreamRuntimeException;
import com.linkedin.datastream.common.ReflectionUtils;
import com.linkedin.datastream.common.VerifiableProperties;


/**
 * Configurations for the DatabaseChunkedReader class.
 */
public class DatabaseChunkedReaderConfig {
  private static final Logger LOG = LoggerFactory.getLogger(DatabaseChunkedReaderConfig.class);
  public static final String DBREADER_DOMAIN_CONFIG = "dbReader";
  public static final String QUERY_TIMEOUT_SECS = "queryTimeout";
  // If the resultSet is 1000 rows, with fetchSize set to 100, it would take 10 network
  // calls to process the entire resultSet. The default fetchSize is 10, which is way too small
  // for both a typical bootstrap scenario.
  public static final String FETCH_SIZE = "fetchSize";
  // Number of rows to chunk on each query. This will limit the number of rows that the server will limit query result to
  public static final String CHUNK_SIZE = "chunk.size";
  public static final String NUM_CHUNK_BUCKETS = "chunk.numBuckets";
  public static final String CHUNK_INDEX = "chunk.index";
  public static final String HASH_FUNCTION = "chunk.hashFunction";
  public static final String CONCAT_FUNCTION = "chunk.concatFunction";
  public static final String DATABASE_INTERPRETER_CLASS_NAME = "database.reader";

  private static final int DEFAULT_QUERY_TIMEOUT_SECS = 0;
  private static final int DEFAULT_FETCH_SIZE = 100;
  private static final long DEFAULT_CHUNK_SIZE = 10000;

  private final int _queryTimeout;
  private final int _fetchSize;
  private final long _chunkSize;
  private final long _numChunkBuckets;
  private final long _chunkIndex;
  private final String _hashFunction;
  private final String _concatFunction;
  private SqlTypeInterpreter _interpreter;

  public DatabaseChunkedReaderConfig(Properties properties) {
    Properties props = new VerifiableProperties(properties).getDomainProperties(DBREADER_DOMAIN_CONFIG);
    VerifiableProperties verifiableProperties = new VerifiableProperties(props);
    _queryTimeout = verifiableProperties.getInt(QUERY_TIMEOUT_SECS, DEFAULT_QUERY_TIMEOUT_SECS);
    _fetchSize = verifiableProperties.getInt(FETCH_SIZE, DEFAULT_FETCH_SIZE);
    _chunkSize = verifiableProperties.getLong(CHUNK_SIZE, DEFAULT_CHUNK_SIZE);
    if (!verifiableProperties.containsKey(NUM_CHUNK_BUCKETS)) {
      String msg = "Number of buckets to use for chunking not specified. Cannot default this config";
      LOG.error(msg);
      throw new DatastreamRuntimeException(msg);
    }
    _numChunkBuckets = verifiableProperties.getLong(NUM_CHUNK_BUCKETS);

    if (!verifiableProperties.containsKey(CHUNK_INDEX)) {
      String msg = "Current chunking index not specified. Cannot default this config";
      LOG.error(msg);
      throw new DatastreamRuntimeException(msg);
    }
    _chunkIndex = verifiableProperties.getLong(CHUNK_INDEX);

    if (!verifiableProperties.containsKey(HASH_FUNCTION)) {
      String msg = "Hash function not specified. Cannot default this config";
      LOG.error(msg);
      throw new DatastreamRuntimeException(msg);
    }
    _hashFunction = verifiableProperties.getString(HASH_FUNCTION);

    if (!verifiableProperties.containsKey(CONCAT_FUNCTION)) {
      String msg = "Concatenation function not specified. Cannot default this config";
      LOG.error(msg);
      throw new DatastreamRuntimeException(msg);
    }
    _concatFunction = verifiableProperties.getString(CONCAT_FUNCTION);

    String tableReader = verifiableProperties.getString(DATABASE_INTERPRETER_CLASS_NAME);
    if (StringUtils.isBlank(tableReader)) {
      String msg = "Database type interpreter class name is not set or is blank";
      LOG.error(msg);
      throw new DatastreamRuntimeException(msg);
    }
    _interpreter = ReflectionUtils.createInstance(tableReader);

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

  public String getHashFunction() {
    return _hashFunction;
  }

  public String getConcatFunction() {
    return _concatFunction;
  }

  public SqlTypeInterpreter getDatabaseInterpreter() {
    return _interpreter;
  }
}
