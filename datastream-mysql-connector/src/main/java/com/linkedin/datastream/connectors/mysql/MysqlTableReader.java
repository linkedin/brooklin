package com.linkedin.datastream.connectors.mysql;

import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.datastream.common.BrooklinEnvelope;
import com.linkedin.datastream.common.BrooklinEnvelopeMetadataConstants;
import com.linkedin.datastream.common.DatastreamRuntimeException;
import com.linkedin.datastream.common.JsonUtils;
import com.linkedin.datastream.connectors.mysql.or.ColumnInfo;
import com.linkedin.datastream.connectors.mysql.or.MysqlQueryUtils;
import com.linkedin.datastream.connectors.mysql.or.TableInfoProvider;


/**
 * A closeable iterator implementation that iterates over all the rows in a table.
 * This implementation buffers rows fetched from the database.
 * <p/>
 * Note: The behavior of this iterator is undefined in the face of concurrent updates to the base data. Also, it is up to the
 * responsibility of the caller to call close explicitly to free up resources.
 */
public class MysqlTableReader {
  private static final Logger LOG = LoggerFactory.getLogger(MysqlTableReader.class);

  private final Connection _conn;
  private final MysqlSource _source;
  private final String _username;
  private final String _password;
  private final List<ColumnInfo> _columnInfo;
  private Statement _stmt;
  private ResultSet _rs;
  private boolean _next;
  // The number of rows fetched from the database when the buffer has to be refilled.
  private final int _bufSize;
  private boolean _isEof;
  private boolean _closed;

  /**
   * Constructs a new {@code MysqlTableReader}.
   *
   */
  public MysqlTableReader(MysqlSource source, String username, String password, TableInfoProvider tableInfoProvider,
      int bufSize) throws SQLException {
    _bufSize = bufSize;
    _source = source;
    _username = username;
    _password = password;
    _columnInfo = tableInfoProvider.getColumnList(source.getDatabaseName(), source.getTableName());
    _conn = MysqlQueryUtils.initMysqlConn(source.getHostName(), source.getPort(), username, password);

    _conn.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
    _stmt = _conn.createStatement();

    String fullyQualifiedTableName = String.format("%s.%s", source.getDatabaseName(), source.getTableName());
    _stmt.executeUpdate(new SqlStringBuilder().append("HANDLER ")
        .append(fullyQualifiedTableName)
        .append(" OPEN AS ")
        .appendIdent(source.getTableName(), "Iterator")
        .toString());
    _rs = _stmt.executeQuery(new SqlStringBuilder().append("HANDLER ")
        .appendIdent(source.getTableName(), "Iterator")
        .append(" READ `PRIMARY` FIRST LIMIT ")
        .append(_bufSize)
        .toString());
    _next = false;
    _closed = false;
    _isEof = false;
  }

  public void close() {
    if (_closed) {
      LOG.debug("Iterator was already closed. Ignoring current close command.");
      return;
    }
    try {
      _rs.close();
      _stmt.executeUpdate(new SqlStringBuilder().append("HANDLER ")
          .appendIdent(_source.getTableName(), "Iterator")
          .append(" CLOSE")
          .toString());
    } catch (SQLException e) {
      LOG.warn("Closing the handler threw an exception", e);
    } finally {
      try {
        _stmt.close();
        _conn.close();
      } catch (SQLException e) {
        LOG.warn("Closing the Mysql connnection threw an exception", e);
      }

      _closed = true;
    }
  }

  private boolean hasNext() throws SQLException {
    if (_isEof || _closed) {
      return false;
    }

    if (_next) {
      return true;
    }

    if (_stmt == null) {
      return false;
    }

    _next = _rs.next();

    if (!_next) {
      _rs.close();
      _rs = _stmt.executeQuery(new SqlStringBuilder().append("HANDLER ")
          .appendIdent(_source.getTableName(), "Iterator")
          .append(" READ `PRIMARY` NEXT LIMIT ")
          .append(_bufSize)
          .toString());
      _next = _rs.next();
      if (!_next) {
        _isEof = true;
      }
    }
    return _next;
  }

  public BrooklinEnvelope next() throws SQLException {

    if (!_isEof || _closed) {
      String msg = "TableReader needs to be closed or it is closed already.";
      LOG.warn(msg);
      throw new DatastreamRuntimeException(msg);
    }

    if (!hasNext()) {
      throw new NoSuchElementException("No more elements in this iterator.");
    }

    try {
      Map<String, String> rowValues = new HashMap<>();
      Map<String, String> keyValues = new HashMap<>();
      for (ColumnInfo column : _columnInfo) {
        rowValues.put(column.getColumnName(), _rs.getString(column.getColumnName()));
        if (column.isKey()) {
          keyValues.put(column.getColumnName(), _rs.getString(column.getColumnName()));
        }
      }
      return buildDatastreamEvent(System.currentTimeMillis(), _source.getDatabaseName(), _source.getTableName(),
          JsonUtils.toJson(keyValues), JsonUtils.toJson(rowValues));
    } finally {
      _next = false;
    }
  }

  private BrooklinEnvelope buildDatastreamEvent(long eventTimestamp, String dbName, String tableName, String key,
      String value) {
    Map<String, String> metadata = new HashMap<>();
    metadata.put(BrooklinEnvelopeMetadataConstants.OPCODE, BrooklinEnvelopeMetadataConstants.OpCode.INSERT.toString());
    metadata.put(BrooklinEnvelopeMetadataConstants.EVENT_TIMESTAMP, String.valueOf(eventTimestamp));
    metadata.put(BrooklinEnvelopeMetadataConstants.DATABASE, dbName);
    metadata.put(BrooklinEnvelopeMetadataConstants.TABLE, tableName);

    return new BrooklinEnvelope(ByteBuffer.wrap(key.getBytes()), ByteBuffer.wrap(value.getBytes()), null, metadata);
  }
}

