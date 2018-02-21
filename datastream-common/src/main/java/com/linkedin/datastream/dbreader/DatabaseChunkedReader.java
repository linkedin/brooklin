package com.linkedin.datastream.dbreader;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;
import javax.sql.DataSource;

import org.apache.avro.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;

import com.linkedin.datastream.avrogenerator.DatabaseSource;
import com.linkedin.datastream.avrogenerator.SchemaGenerationException;
import com.linkedin.datastream.common.SqlTypeInterpreter;
import com.linkedin.datastream.common.DatabaseRow;
import com.linkedin.datastream.common.DatastreamRuntimeException;


/**
 * Generic JDBC Source Reader. Can be used for executing a generic query on the given source using chunking algorithm.
 * The reader acts as an iterator for Database records and returns a DatabaseRow record on each poll and will perform
 * the next chunked query as required.
 * A typical flow to use the reader would look like this:
 *    DatabaseChunkedReader reader = new DatabaseChunkedReader (...)
 *    reader.start()
 *    boolean eof = false;
 *    while(eof) {
 *      DatabaseRow record = reader.poll()
 *      if (record == null) {
 *        eof = true;
 *      }
 *      processRecord(record);
 *    }
 *
 *  Chunking is done at two levels. A hash is used to process only the keys that hash to the supplied index.
 *  This helps running multiple reader instances in parallel and each can process only keys that hash to
 *  a specific value. Since number of keys that hash to a given value could potentially be very large,
 *  chunking is also done on number of rows returned per query. To control the number of records returned
 *  in the query, the ROWNUM limit is specified in the query. To ignore rows already seen, we need to order
 *  the records by some unique key column(s), and ignore rows with keys smaller than or equal to max value(s)
 *  seen in the previous query. The unique key column(s) chosen and the specific order in which to ORDER them
 *  by is critical to the correctness of the algorithm and the reader relies on the DatabaseSource to be
 *  able to supply that information accurately, along with the schema to read the records in. Query hints might
 *  also need to be provided in the inner query to ensure the indexes are being used for the queries and hence will
 *  be more performant.
 */
public class DatabaseChunkedReader {
  private static final Logger LOG = LoggerFactory.getLogger(DatabaseChunkedReader.class);

  private final DataSource _dataSource;
  private final DatabaseSource _databaseSource;
  private final Connection _connection;

  private final DBReaderConfig _dbReaderConfig;
  private final long _chunkSize;
  private final long _maxChunkIndex;
  private final long _chunkIndex;
  private final int _queryTimeoutSecs;
  private final int _fetchSize;
  private final String _hashFunction;
  private final String _concatFunction;

  private final String _sourceQuery;
  private final String _readerId;
  private final String _chunkingTable;

  // Ordered list of keys and max values seen in previous query, needed for chunking.
  // The order is based on the index definition in the Database.
  private final LinkedHashMap<String, Object> _chunkingKeys = new LinkedHashMap<>();
  private String _chunkingKeysString;
  private int _numChunkingKeys;
  private boolean _endOfFile = false;
  private String _chunkedQuery;
  private PreparedStatement _firstStmt;
  private PreparedStatement _queryStmt;
  private ResultSet _queryResultSet;
  private long numRowsInResult = 0;
  private Schema _tableSchema;
  private SqlTypeInterpreter _interpreter;

  /**
   * Create a DatabaseChunkedReader instance
   * @param props Configuration
   * @param source JDBC DataSource object to use for connecting
   * @param sourceQuery Query to execute on the source in chunked mode. The query cannot be any SQL query and
   *                    needs to follow specific rules. The query should only involve one table, as specified
   *                    by the 'table' parameter, should query all unique key columns and in the order specified
   *                    in the Index for the table.
   * @param table table to use for getting unique key column(s) information to add the chunking predicate
   * @param databaseSource DatabaseSource implementation to query table metadata needed for constructing the chunk query
   * @param id Name to identify the reader instance in logs
   */
  public DatabaseChunkedReader(Properties props, DataSource source, String sourceQuery, String table,
      DatabaseSource databaseSource, String id) throws SQLException, SchemaGenerationException {
    _dbReaderConfig = new DBReaderConfig(props);
    _sourceQuery = sourceQuery;
    _dataSource = source;
    _databaseSource = databaseSource;
    _readerId = id;
    _chunkingTable = table;
    _fetchSize = _dbReaderConfig.getFetchSize();
    _queryTimeoutSecs = _dbReaderConfig.getQueryTimeout();
    _maxChunkIndex = _dbReaderConfig.getNumChunkBuckets() - 1;
    _chunkSize = _dbReaderConfig.getChunkSize();
    _chunkIndex = _dbReaderConfig.getChunkIndex();
    _connection = source.getConnection();
    _hashFunction = _dbReaderConfig.getHashFunction();
    _concatFunction = _dbReaderConfig.getConcatFunction();
    _interpreter = _dbReaderConfig.getDatabaseInterpreter();
    getChunkingKeyInfo();
    validateQuery(sourceQuery);
  }

  private void getChunkingKeyInfo() throws SQLException, SchemaGenerationException {
    _databaseSource.getPrimaryKeyFields(_chunkingTable).stream().forEach(k -> _chunkingKeys.put(k, null));
    if (_chunkingKeys.isEmpty()) {
      String msg = "Failed to get primary keys for table " + _chunkingTable +
          ". Cannot chunk without it";
      LOG.error(msg);
      throw new DatastreamRuntimeException(msg);
    }

    StringBuilder tmp = new StringBuilder();
    Iterator<String> iter = _chunkingKeys.keySet().iterator();
    tmp.append(iter.next());
    while (iter.hasNext()) {
      tmp.append("," + iter.next());
    }

    _chunkingKeysString = tmp.toString();
    _numChunkingKeys = _chunkingKeys.size();
  }

  /**
   * Throws DatastreamRuntimeException if invalid query.
   * Query is invalid if
   * 1. It has a join
   * 2. All the primary keys are not part of the selected columns
   * 3. If there is no ORDER BY clause or if they are not ordered in the same order as a unique index.
   * 4. If the table in the inner query doesnt match the table supplied in the reader parameter
   * @param query
   */
  private void validateQuery(String query) {
    // todo : actually implement this. May be there are better libraries to use than string parsing
  }

  // todo : add a token generator for use with the following query generating APIs. This will allow for writing robust
  // unit-tests.

  /**
   * With chunking, the first query cannot ignore any key values and subsequent ones will ignore keys smaller than
   * max seen in previous query. So we need a different query the first time.
   * @throws SQLException
   */
  private void executeFirstChunkedQuery() throws SQLException {
    StringBuilder firstQuery = new StringBuilder();
    firstQuery.append("SELECT * FROM (");
    firstQuery.append(_sourceQuery);
    firstQuery.append(")");
    firstQuery.append("WHERE ORA_HASH(");
    if (_numChunkingKeys > 1) {
      firstQuery.append("CONCAT(" + _chunkingKeysString + ")");
    } else {
      firstQuery.append(_chunkingKeysString);
    }
    firstQuery.append(" , " + _maxChunkIndex + " ) = " + _chunkIndex + " ");
    firstQuery.append("AND ROWNUM <= " + _chunkSize);

    _firstStmt = _connection.prepareStatement(firstQuery.toString());
    _firstStmt.setFetchSize(_fetchSize);
    _firstStmt.setQueryTimeout(_queryTimeoutSecs);
    _queryResultSet = _firstStmt.executeQuery();
  }

  @VisibleForTesting
  static String generateKeyChunkingPredicate(LinkedHashMap<String, Object> keyMap) {
    StringBuilder str = new StringBuilder();
    int numkeys = keyMap.size();
    str.append("(");
    Iterator<String> lsbKeys = keyMap.keySet().iterator();
    str.append(" ( " + lsbKeys.next() + " > ? )");
    for (int i = 1; i < numkeys; i++) {
      str.append(" OR ( ");
      Iterator<String> msbKeys = keyMap.keySet().iterator();
      for (int j = 0; j < i; j++) {
        str.append(msbKeys.next() + " = ? AND ");
      }
      str.append(lsbKeys.next() + " > ? )");
    }
    str.append(" )");
    return str.toString();
  }

  @VisibleForTesting
  static String getChunkedQuery(String nestedQuery, String pkeyString, LinkedHashMap<String, Object> keyMap,
      long chunkSize, long bucketSize, long chunkIndex, String hashFunction, String concatFunction) {
    StringBuilder query = new StringBuilder();
    query.append("SELECT * FROM ( ");
    query.append(nestedQuery);
    query.append(" ) ");
    query.append("WHERE " + hashFunction + " ( ");
    int count = keyMap.size();

    if (count > 1) {
      query.append(concatFunction + " ( " + pkeyString + " )");
    } else {
      query.append(pkeyString);
    }
    query.append(" , " + bucketSize + " ) = " + chunkIndex + " AND ");
    query.append(generateKeyChunkingPredicate(keyMap));
    query.append(" AND ROWNUM <= " + chunkSize);
    return query.toString();
  }

  /**
   * Fill in the key values from previous query result
   */
  private void prepareChunkedQuery() throws SQLException {
    int numkeys = _chunkingKeys.size();
    Iterator<String> lsbKeys = _chunkingKeys.keySet().iterator();
    _queryStmt.setObject(1, _chunkingKeys.get(lsbKeys.next()));
    int count = 2;
    for (int i = 1; i < numkeys; i++) {
      Iterator<String> msbKeys = _chunkingKeys.keySet().iterator();
      for (int j = 0; j < i; j++) {
        _queryStmt.setObject(count++, _chunkingKeys.get(msbKeys.next()));
      }
      _queryStmt.setObject(count++, _chunkingKeys.get(lsbKeys.next()));
    }
  }

  private void executeChunkedQuery() throws SQLException {
    _queryResultSet = _queryStmt.executeQuery();
  }

  private void releaseResources(String msg) {
    LOG.info(msg);
    LOG.info("Releasing resources");
    if (_queryResultSet != null) {
      try {
        _queryResultSet.close();
      } catch (SQLException e) {
        LOG.warn("Failed to close ResultSet for reader {}. Might cause resource leak", _readerId, e);
      }
    }

    if (_firstStmt != null) {
      try {
        _firstStmt.close();
      } catch (SQLException e) {
        LOG.warn("Failed to close Statement for reader {}. Might cause resource leak", _readerId, e);
      }
    }

    if (_queryStmt != null) {
      try {
        _queryStmt.close();
      } catch (SQLException e) {
        LOG.warn("Failed to close PreparedStatement for reader {}. Might cause resource leak", _readerId, e);
      }
    }
  }

  /**
   * Prepare reader for poll.
   * @throws SQLException
   */
  public void start() throws SQLException {
    _chunkedQuery = getChunkedQuery(_sourceQuery, _chunkingKeysString, _chunkingKeys,
    _chunkSize, _maxChunkIndex, _chunkIndex, _hashFunction, _concatFunction);
    _tableSchema = _databaseSource.getTableSchema(_chunkingTable);
  }

  private DatabaseRow getNextRow() throws SQLException {
    // todo Fix to use lambda with unchecked exception if possible
    Iterator<Map.Entry<String, Object>> iter = _chunkingKeys.entrySet().iterator();
    Map.Entry<String, Object> entry;
    for (int i = 0; i < _numChunkingKeys; i++) {
      entry = iter.next();
      _chunkingKeys.put(entry.getKey(), _queryResultSet.getObject(entry.getKey()));
    }
    numRowsInResult++;

    ResultSetMetaData rsmd = _queryResultSet.getMetaData();
    int colCount = rsmd.getColumnCount();
    DatabaseRow row = new DatabaseRow();
    for (int i = 1; i <= colCount; i++) {
      String colName = rsmd.getColumnName(i);
      int colType = rsmd.getColumnType(i);
      String formattedColName = _interpreter.formatColumn(colName);
      Object result = _interpreter.sqlObjectToAvro(_queryResultSet.getObject(i),
          formattedColName, _tableSchema);
      row.addField(formattedColName, result, colType);
    }
    return row;
  }

  /**
   * Poll for the next row in the DB. Makes a call to server if all cached messages have been served.
   * It acts as a buffered reader, performing more queries if the previous query returned chunk size rows.
   * @return The next row from the DB as served by the query constraints.
   * @throws SQLException
   */
  public DatabaseRow poll() throws SQLException {
    if (_queryResultSet == null) { // If first poll
      executeFirstChunkedQuery();

      // Prepare chunked query for next round
      _queryStmt = _connection.prepareStatement(_chunkedQuery);
      _queryStmt.setFetchSize(_fetchSize);
      _queryStmt.setQueryTimeout(_queryTimeoutSecs);
    }

    if (!_queryResultSet.next()) {
      if (_endOfFile) {
        return null;
      }

      if (numRowsInResult < _chunkSize) {
        _endOfFile = true;
        return null;
      }

      // Perform the next chunked query
      numRowsInResult = 0;
      prepareChunkedQuery();
      executeChunkedQuery();

      // Previous query might have returned exactly ChunkSize num rows, and there are no more records left
      if (!_queryResultSet.next()) {
        _endOfFile = true;
        return null;
      }
    }

    return getNextRow();
  }

  /**
   * Only API that will not rethrow SQLException. Will swallow error and print an error log
   */
  public void close() {
    releaseResources("Reader close invoked.");
  }
}

