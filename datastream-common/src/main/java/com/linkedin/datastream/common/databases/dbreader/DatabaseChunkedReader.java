package com.linkedin.datastream.common.databases.dbreader;

import java.io.Closeable;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import javax.sql.DataSource;

import org.apache.avro.Schema;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.datastream.avrogenerator.DatabaseSource;
import com.linkedin.datastream.avrogenerator.SchemaGenerationException;
import com.linkedin.datastream.common.DatastreamRuntimeException;
import com.linkedin.datastream.common.ErrorLogger;
import com.linkedin.datastream.common.databases.DatabaseRow;
import com.linkedin.datastream.metrics.BrooklinMetricInfo;

/**
 * Generic JDBC Source Reader. Can be used for executing a generic query on the given source using chunking algorithm.
 * The reader acts as an iterator for Database records and returns a GenericRecord representation a row per poll and
 * will perform the next chunked query as required.
 * A typical flow to use the reader would look like this:
 * <pre>
 *  try {
 *    DatabaseChunkedReader reader = new DatabaseChunkedReader (...)
 *    reader.start(null)
 *    for (GenericRecord record = reader.poll(); record != null; record = reader.poll()) {
 *      processRecord(record);
 *    }
 *  }
 * </pre>
 *
 *  Chunking is done at two levels. A hash is used to process only the keys that hash to the supplied index.
 *  This helps running multiple reader instances in parallel and each can process only keys that hash to a
 *  specific value. Since number of keys that hash to a given value could potentially be very large, chunking
 *  is also done on number of rows returned per query. To control the number of records returned in the query,
 *  the row limit is specified in the query. To ignore rows already seen, we need to order the records by some
 *  unique key column(s), and ignore rows with keys smaller than max value(s) seen in the previous query.
 *  The unique key column(s) chosen and the specific order in which to ORDER them by is critical to the correctness
 *  of the algorithm and the reader relies on the DatabaseSource to be able to supply that information accurately,
 *  along with the schema to read the records in. Query hints might also need to be provided in the inner query
 *  to ensure the indexes are being used for the queries and hence will be more performant.
 */
public class DatabaseChunkedReader implements Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(DatabaseChunkedReader.class);

  private final DatabaseSource _databaseSource;
  private final Connection _connection;

  private final DatabaseChunkedReaderConfig _databaseChunkedReaderConfig;
  // Max number of rows to fetch for each query. This will help the server limit the number of full row
  // fetches that it has to do. For example in Oracle, a ROWNUM <= 1000 will add a stopKey constraint where the DB will
  // only look for first 1000 matches that match the specified constraints and will do a full row fetch only for these.
  private final long _rowCountLimit;
  private final List<Integer> _partitions = new ArrayList<>();
  private final int _queryTimeoutSecs;
  // If the ResultSet is 10000 rows, with fetchSize set to 1000, it would take 10 network calls to fetch the entire
  // ResultSet from the server.
  private final int _fetchSize;
  private final String _sourceQuery;
  private final String _readerId;
  private final String _database;
  private final String _table;
  private final ChunkedQueryManager _chunkedQueryManager;
  private final boolean _skipBadMessagesEnabled;

  // Ordered list of keys and max values seen in previous query, needed for chunking.
  // The order is based on the index definition in the Database.
  private final LinkedHashMap<String, Object> _chunkingKeys = new LinkedHashMap<>();

  private boolean _initialized = false;
  private int _numPartitions;
  private String _chunkedQuery;
  private PreparedStatement _firstStmt;
  private PreparedStatement _queryStmt;
  private ResultSet _queryResultSet;
  private long _numRowsInResult = 0;
  private Schema _tableSchema;

  private DatabaseChunkedReaderMetrics _metrics;

  /**
   * Create a DatabaseChunkedReader instance
   * @param props Configuration
   * @param source JDBC DataSource object to use for connecting
   * @param sourceQuery Query to execute on the source in chunked mode. The query cannot be any SQL query and
   *                    needs to follow specific rules. The query should only involve one table, as specified
   *                    by the 'table' parameter, should query all unique key columns and in the order specified
   *                    in the Index for the table.
   * @param db Database that the DataSource is connected to. If null, connection string from DataSource is used to
   *           derive the string. Only used for creating metric names.
   * @param table table to use for getting unique key column(s) information to add the chunking predicate. This should
   *              be the same as the table being read as part of the sourceQuery
   * @param databaseSource DatabaseSource implementation to query table metadata needed for constructing the chunk query
   * @param id Name to identify the reader instance in logs
   */
  public DatabaseChunkedReader(Properties props, DataSource source, String sourceQuery, String db, String table,
      DatabaseSource databaseSource, String id) throws SQLException {
    _databaseChunkedReaderConfig = new DatabaseChunkedReaderConfig(props);
    _sourceQuery = sourceQuery;
    _databaseSource = databaseSource;
    _readerId = id;
    _table = table;
    _fetchSize = _databaseChunkedReaderConfig.getFetchSize();
    _queryTimeoutSecs = _databaseChunkedReaderConfig.getQueryTimeout();
    _rowCountLimit = _databaseChunkedReaderConfig.getRowCountLimit();
    _connection = source.getConnection();
    Validate.notNull(_connection, "getConnection returned null for source" + source);
    _chunkedQueryManager = _databaseChunkedReaderConfig.getChunkedQueryManager();
    _skipBadMessagesEnabled = _databaseChunkedReaderConfig.shouldSkipBadMessage();

    if (StringUtils.isBlank(db)) {
      _database = _connection.getMetaData().getUserName();
      LOG.warn("Database name not specified. Using name derived from connection's usename {}",
          _database);
    } else {
      _database = db;
    }

    validateQuery(sourceQuery);
  }

  private void initializeDatabaseMetadata(List<Integer> partitions) throws SQLException, SchemaGenerationException {
    // Verify the partitions are valid.
    _numPartitions = _databaseSource.getPartitionCount();
    partitions.forEach(p -> Validate.isTrue(p >= 0 && p < _numPartitions));

    _databaseSource.getPrimaryKeyFields(_table).stream().forEach(k -> _chunkingKeys.put(k, null));
    if (_chunkingKeys.isEmpty()) {
      _metrics.updateErrorRate();

      // There can be tables without primary keys. Let user to handle it.
      String msg = "Failed to get primary keys for table " + _table + ". Cannot chunk without it";
      throw new DatastreamRuntimeException(msg, new InvalidKeyException());
    }

    _tableSchema = _databaseSource.getTableSchema(_table);
    if (_tableSchema == null) {
      ErrorLogger.logAndThrowDatastreamRuntimeException(LOG, "Failed to get schema for table " + _table);
    }
  }

  private void validateQuery(String query) {
    _chunkedQueryManager.validateQuery(query);
  }

  private void generateChunkedQueries() throws SQLException {
    String firstQuery =
        _chunkedQueryManager.generateFirstQuery(_sourceQuery, new ArrayList<String>(_chunkingKeys.keySet()), _rowCountLimit,
            _numPartitions, _partitions);
    _firstStmt = _connection.prepareStatement(firstQuery);
    _firstStmt.setFetchSize(_fetchSize);
    _firstStmt.setQueryTimeout(_queryTimeoutSecs);

    _chunkedQuery =
        _chunkedQueryManager.generateChunkedQuery(_sourceQuery, new ArrayList<String>(_chunkingKeys.keySet()), _rowCountLimit,
            _numPartitions, _partitions);
    _queryStmt = _connection.prepareStatement(_chunkedQuery);
    _queryStmt.setFetchSize(_fetchSize);
    _queryStmt.setQueryTimeout(_queryTimeoutSecs);
  }

  /**
   * Fill in the key values from previous query result
   */
  private void prepareChunkedQuery(PreparedStatement stmt, List<Object> keys) throws SQLException {
    _chunkedQueryManager.prepareChunkedQuery(stmt, keys);
  }

  private void executeChunkedQuery(PreparedStatement stmt) throws SQLException {
    long timeStart = System.currentTimeMillis();
    _queryResultSet = stmt.executeQuery();
    _metrics.updateQueryExecutionDuration(System.currentTimeMillis() - timeStart);
    _metrics.updateQueryExecutionRate();
  }

  private void executeFirstChunkedQuery() throws SQLException {
    // Based on checkpoint state execute the first chunked query or the chunked query with checkpointed key values
    boolean checkpointsSaved = _chunkingKeys.values().iterator().next() != null;
    if (checkpointsSaved) {
      executeNextChunkedQuery();
    } else {
      executeChunkedQuery(_firstStmt);
    }
  }

  private void executeNextChunkedQuery() throws SQLException {
    prepareChunkedQuery(_queryStmt, new ArrayList<>(_chunkingKeys.values()));
    executeChunkedQuery(_queryStmt);
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

    _metrics.deregister();
    _initialized = false;
  }

  /**
   * Prepare reader for poll. Calling start on a reader multiple times, is not allowed.
   * A valid flow: subscribe() -> poll().* -> close()
   * @param checkpoint Row offset to start reading from. The row greater than the checkpointed row will be returned by
   *                   the first poll. The reader can be made to start reading from a specific row which identified
   *                   uniquely by the primary[/unique] key columns having values as specified in the checkpoint map.
   *                   If null, table is read from from the first row ordered by the primary keys.
   *                   For example the table with primary key columns K1 and K2, having a row with K1 = V1 and K2 = V2
   *                   can be read starting at row immediately following this row by specifying the map {k1=V1, K2=V2}
   * @throws SQLException
   * @throws SchemaGenerationException
   */
  public void subscribe(List<Integer> partitions, Map<String, Object> checkpoint) throws SQLException, SchemaGenerationException {
    if (_initialized) {
      throw new DatastreamRuntimeException("Subscribing an already subscribed reader");
    }

    _metrics = new DatabaseChunkedReaderMetrics(String.join(".", _database, _table), _readerId);
    _partitions.addAll(partitions);
    initializeDatabaseMetadata(partitions);
    generateChunkedQueries();
    loadCheckpoint(checkpoint);
    _initialized = true;
  }

  private void loadCheckpoint(Map<String, Object> checkpoint) {
    if (checkpoint == null || checkpoint.isEmpty()) {
      LOG.warn("No checkpoints supplied. Skipping checkpoint load");
      return;
    }

    Validate.isTrue(checkpoint.size() == _chunkingKeys.size(),
        String.format("Load checkpoint called with %s keys when expected %s. Checkpoint supplied %s",
            checkpoint.size(), _chunkingKeys.size(), checkpoint));
    _chunkingKeys.keySet().forEach(k -> {
      Validate.isTrue(checkpoint.containsKey(k),
          String.format("Load checkpoint called without key %s. Checkpoint map supplied : %s", k, checkpoint));
      _chunkingKeys.put(k, checkpoint.get(k));
    });
  }


  private DatabaseRow getNextRow() throws SQLException {
    _numRowsInResult++;
    try {
      ResultSetMetaData rsmd = _queryResultSet.getMetaData();
      int colCount = rsmd.getColumnCount();
      DatabaseRow payloadRecord = new DatabaseRow();
      for (int i = 1; i <= colCount; i++) {
        String colName = rsmd.getColumnName(i);
        Object column = _queryResultSet.getObject(i);
        payloadRecord.addField(colName, column, rsmd.getColumnType(i));
        // If column is one of the key values, save the result from query to perform chunking query in the future
        if (_chunkingKeys.containsKey(colName)) {
          if (column == null) {
            ErrorLogger.logAndThrowDatastreamRuntimeException(LOG,  colName + " field is not expected to be null");
          }
          _chunkingKeys.put(colName, column);
        }
      }
      return payloadRecord;
    } catch (SQLException e) {
      _metrics.updateErrorRate();

      if (_skipBadMessagesEnabled) {
        LOG.warn("Skipping row due to SQL exception", e);
        _metrics.updateSkipBadMessagesRate();
      } else {
        ErrorLogger.logAndThrowDatastreamRuntimeException(LOG, "Failed to interpret row and skipBadMessage not enabled", e);
      }
      return null;
    }
  }

  /**
   * Poll for the next row in the DB. Makes a call to server if all cached messages have been served.
   * It acts as a buffered reader, performing more queries if the previous query returned chunk size rows.
   * Client should call start before poll. Calling start without reading all records will result in resetting the reader
   * back to start after releasing resources.
   * @return Null if end of records or the next row from the DB as served by the query constraints. Record is returned as
   *         a GenericRecord with schema specified per getTableSchema call on the DatabaseSource for the source table.
   * @throws SQLException
   */
  public DatabaseRow poll() throws SQLException {
    if (!_initialized) {
      throw new DatastreamRuntimeException("Cannot poll on unsubscribed reader. Call subscribe() first");
    }

    if (_queryResultSet == null) {
      executeFirstChunkedQuery();
    }

    DatabaseRow row = null;
    while (row == null) {
      if (!_queryResultSet.next()) {
        // If previous query read less than requested chunks, we are at the end of the table.
        // No more chunks to fetch, indicate end of records.
        if (_numRowsInResult < _rowCountLimit) {
          return null;
        }

        // Perform the next chunked query
        _numRowsInResult = 0;
        executeNextChunkedQuery();
        if (!_queryResultSet.next()) {
          return null;
        }
      }
      row = getNextRow();
    }
    return row;
  }

  /**
   * Only API that will not rethrow SQLException. Will swallow error and print an error log.
   */
  public void close() {
    releaseResources("Reader close invoked.");
  }

  public static List<BrooklinMetricInfo> getMetricInfos() {
    return DatabaseChunkedReaderMetrics.getMetricInfos();
  }
}
