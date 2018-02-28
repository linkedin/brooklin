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
import java.util.Properties;
import javax.sql.DataSource;

import org.apache.avro.Schema;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.datastream.avrogenerator.DatabaseSource;
import com.linkedin.datastream.avrogenerator.SchemaGenerationException;
import com.linkedin.datastream.common.DatastreamRuntimeException;
import com.linkedin.datastream.common.SqlTypeInterpreter;
import com.linkedin.datastream.common.databases.DatabaseRow;
import com.linkedin.datastream.metrics.BrooklinMetricInfo;


/**
 * Generic JDBC Source Reader. Can be used for executing a generic query on the given source using chunking algorithm.
 * The reader acts as an iterator for Database records and returns a DatabaseRow record on each poll and will perform
 * the next chunked query as required.
 * A typical flow to use the reader would look like this:
 * <pre>
 *  try {
 *    DatabaseChunkedReader reader = new DatabaseChunkedReader (...)
 *    reader.start()
 *    for (DatabaseRow record = reader.poll(); record != null; record = reader.poll()) {
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

  private final DataSource _dataSource;
  private final DatabaseSource _databaseSource;
  private final Connection _connection;

  private final DatabaseChunkedReaderConfig _databaseChunkedReaderConfig;
  // Max number of rows to fetch for each query. This will help the server limit the number of full row
  // fetches that it has to do. For example in Oracle, a ROWNUM <= 1000 will add a stopKey constraint where the DB will
  // only look for first 1000 matches that match the specified constraints and will do a full row fetch only for these.
  private final long _rowCountLimit;
  private final long _maxChunkIndex;
  private final long _chunkIndex;
  private final int _queryTimeoutSecs;
  // If the ResultSet is 10000 rows, with fetchSize set to 1000, it would take 10 network calls to fetch the entire
  // ResultSet from the server. The default fetchSize of 10 is way too small for a typical bootstrap scenario.
  private final int _fetchSize;
  private final String _sourceQuery;
  private final String _readerId;
  private final String _database;
  private final String _chunkingTable;
  private final ChunkedQueryManager _chunkedQueryManager;
  private final boolean _skipBadMessagesEnabled;

  // Ordered list of keys and max values seen in previous query, needed for chunking.
  // The order is based on the index definition in the Database.
  private final LinkedHashMap<String, Object> _chunkingKeys = new LinkedHashMap<>();

  private boolean _initialized = false;
  private String _chunkedQuery;
  private PreparedStatement _firstStmt;
  private PreparedStatement _queryStmt;
  private ResultSet _queryResultSet;
  private long _numRowsInResult = 0;
  private Schema _tableSchema;
  private SqlTypeInterpreter _interpreter;

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
    _dataSource = source;
    _databaseSource = databaseSource;
    _readerId = id;
    _chunkingTable = table;
    _fetchSize = _databaseChunkedReaderConfig.getFetchSize();
    _queryTimeoutSecs = _databaseChunkedReaderConfig.getQueryTimeout();
    _maxChunkIndex = _databaseChunkedReaderConfig.getNumChunkBuckets() - 1;
    _rowCountLimit = _databaseChunkedReaderConfig.getRowCountLimit();
    _chunkIndex = _databaseChunkedReaderConfig.getChunkIndex();
    _connection = source.getConnection();
    _interpreter = _databaseChunkedReaderConfig.getDatabaseInterpreter();
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

  private void initializeChunkingKeyInfo() throws SQLException, SchemaGenerationException {
    _databaseSource.getPrimaryKeyFields(_chunkingTable).stream().forEach(k -> _chunkingKeys.put(k, null));
    if (_chunkingKeys.isEmpty()) {
      _metrics.updateErrorRate();

      String msg = "Failed to get primary keys for table " + _chunkingTable + ". Cannot chunk without it";
      LOG.error(msg);
      throw new DatastreamRuntimeException(msg);
    }
  }

  private void validateQuery(String query) {
    _chunkedQueryManager.validateQuery(query);
  }

  private void executeFirstChunkedQuery() throws SQLException {
    String firstQuery =
        _chunkedQueryManager.generateFirstQuery(_sourceQuery, new ArrayList<>(_chunkingKeys.keySet()), _rowCountLimit,
            _maxChunkIndex, _chunkIndex);
    _firstStmt = _connection.prepareStatement(firstQuery);
    _firstStmt.setFetchSize(_fetchSize);
    _firstStmt.setQueryTimeout(_queryTimeoutSecs);

    executeChunkedQuery(_firstStmt);
  }

  /**
   * Fill in the key values from previous query result
   */
  private void prepareChunkedQuery() throws SQLException {
    _chunkedQueryManager.prepareChunkedQuery(_queryStmt, new ArrayList<>(_chunkingKeys.values()));
  }

  private void executeChunkedQuery(PreparedStatement stmt) throws SQLException {
    long timeStart = System.currentTimeMillis();
    _queryResultSet = stmt.executeQuery();
    _metrics.updateQueryExecutionDuration(System.currentTimeMillis() - timeStart);
    _metrics.updateQueryExecutionRate();
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
   * Prepare reader for poll. Calling start on a reader multiple times, is allowed and will release previous resources
   * and reset the reader to prepare for another round of reading the table from start.
   * So all of these are acceptible flows:
   *  start -> poll -> poll -> close;
   *  start -> poll -> poll -> close -> start -> poll .. -> close;
   *  start -> poll -> poll -> start -> poll .. -> close -> start -> poll .. -> close;
   *  start -> close -> start -> poll .. -> close
   * @throws SQLException
   */
  public void start() throws SQLException, SchemaGenerationException {
    if (_initialized) {
      close();
    }

    _metrics = new DatabaseChunkedReaderMetrics(String.join(".", _database , _chunkingTable), _readerId);

    initializeChunkingKeyInfo();
    _chunkedQuery =
        _chunkedQueryManager.generateChunkedQuery(_sourceQuery, new ArrayList<>(_chunkingKeys.keySet()), _rowCountLimit,
            _maxChunkIndex, _chunkIndex);
    _tableSchema = _databaseSource.getTableSchema(_chunkingTable);
    _initialized = true;
  }

  private DatabaseRow getNextRow() throws SQLException {
    _numRowsInResult++;
    try {
      ResultSetMetaData rsmd = _queryResultSet.getMetaData();
      int colCount = rsmd.getColumnCount();
      DatabaseRow row = new DatabaseRow();
      for (int i = 1; i <= colCount; i++) {
        String colName = rsmd.getColumnName(i);
        int colType = rsmd.getColumnType(i);
        String formattedColName = _interpreter.formatColumnName(colName);
        Object result = _interpreter.sqlObjectToAvro(_queryResultSet.getObject(i), formattedColName, _tableSchema);
        row.addField(formattedColName, result, colType);
        if (_chunkingKeys.containsKey(colName)) {
          _chunkingKeys.put(colName, result);
        }
      }
      return row;
    } catch (SQLException e) {
      _metrics.updateErrorRate();

      if (_skipBadMessagesEnabled) {
        LOG.warn("Skipping row due to SQL exception", e);
        _metrics.updateSkipBadMessagesRate();
        return null;
      } else {
        LOG.error("Failed to interpret row and skipBadMessage not enabled", e);
        throw e;
      }
    }
  }

  /**
   * Poll for the next row in the DB. Makes a call to server if all cached messages have been served.
   * It acts as a buffered reader, performing more queries if the previous query returned chunk size rows.
   * Client should call start before poll. Calling start without reading all records will result in resetting the reader
   * back to start after releasing resources.
   * @return The next row from the DB as served by the query constraints and null if end of records
   * @throws SQLException
   */
  public DatabaseRow poll() throws SQLException {
    if (!_initialized) {
      throw new DatastreamRuntimeException("call start before calling poll for records");
    }

    if (_queryResultSet == null) { // If first poll
      executeFirstChunkedQuery();

      // Prepare chunked query for next round
      _queryStmt = _connection.prepareStatement(_chunkedQuery);
      _queryStmt.setFetchSize(_fetchSize);
      _queryStmt.setQueryTimeout(_queryTimeoutSecs);
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
        prepareChunkedQuery();
        executeChunkedQuery(_queryStmt);
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

  public List<BrooklinMetricInfo> getMetricInfos() {
    return DatabaseChunkedReaderMetrics.getMetricInfos();
  }
}