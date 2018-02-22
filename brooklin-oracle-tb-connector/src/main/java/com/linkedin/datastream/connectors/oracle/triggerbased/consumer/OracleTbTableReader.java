package com.linkedin.datastream.connectors.oracle.triggerbased.consumer;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.NoSuchElementException;

import org.apache.avro.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.datastream.common.DatastreamException;
import com.linkedin.datastream.common.DatastreamRuntimeException;
import com.linkedin.datastream.common.OracleTableReader;


/**
 * Once we have all the rows that changed from a specific Oracle Source
 * we need to iterate through the Result set and build an OracleChangeEvent for each changed row
 *
 * This class exposes two public methods that act as an iterator for the resultSet, it is
 * designed to be used by the OracleConsumer. Each OracleChangeEvent is composed of
 * Records, which are simple key value pairs representing column name and value.
 *
 */
public class OracleTbTableReader {
  private final static int SCN_INDEX = 1;
  private final static int TIMESTAMP_INDEX = 2;


  private static final Logger LOG = LoggerFactory.getLogger(OracleTbTableReader.class);
  private ResultSet _resultSet;
  private boolean _next;
  private boolean _isEof;
  private Schema _schema;
  private static final OracleTableReader ORACLE_TYPE_INTERPRETER = new OracleTableReader();

  public OracleTbTableReader(ResultSet resultSet, Schema schema) {
    _resultSet = resultSet;
    _isEof = false;
    _next = false;
    _schema = schema;
  }

  /**
   * @return Boolean indicating if there are any more changed rows that have not been converted to datastreamEvents
   */
  public boolean hasNext() throws SQLException {
    if (_isEof) {
      return false;
    }

    if (_next) {
      return true;
    }

    _next = _resultSet.next();

    if (!_next) {
      _isEof = true;
    }

    return _next;
  }

  public OracleChangeEvent next() throws SQLException, DatastreamException {
    if (!hasNext()) {
      throw new NoSuchElementException("No more elements in this iterator.");
    }

    _next = false;

    return generateOracleChangeEvent();
  }

  /**
   * Recursively parse the element stored in a specific resultSet tuple.
   * Given the ResultSet returned from the SQL query, we convert that ResultSet into an OracleChangeEvent
   * Note, that one row of the ResultSet represents one OracleChangeEvent instance.
   * @return an instance of an OracleChangeEvent
   * @throws SQLException
   */
  private OracleChangeEvent generateOracleChangeEvent() throws SQLException, DatastreamException {

    long scn = _resultSet.getLong(SCN_INDEX);
    Timestamp ts = _resultSet.getTimestamp(TIMESTAMP_INDEX);
    long sourceTimestamp = ts.getTime();

    OracleChangeEvent oracleChangeEvent = null;

    try {
      oracleChangeEvent = generateEvent(_resultSet, _schema, scn, sourceTimestamp, 3);
    } catch (Exception e) {
      String msg = String.format("Failed to process ResultSet with SCN: %s, Timestamp: %s, Schema: %s",
          scn,
          ts,
          _schema.getFullName());

      LOG.error(msg, e);
      throw new DatastreamException(msg, e);
    }

    return oracleChangeEvent;
  }

  /**
   * Each OracleChangeEvent is composed of multiple DatabaseColumnRecords
   * where each DatabaseColumnRecord maps directly to specific tuple returned
   * from the query resultSet. When building the Record, we need the type of
   * the record to match the type expected from the Avro Schema for that specific field.
   */
  protected static OracleChangeEvent generateEvent(ResultSet rs, Schema avroSchema, long scn, long sourceTimestamp,
      int startIndex)
      throws SQLException {
    OracleChangeEvent event = new OracleChangeEvent(scn, sourceTimestamp);

    ResultSetMetaData rsmd = rs.getMetaData();
    int colCount = rsmd.getColumnCount();

    if (colCount < 3) {
      throw new DatastreamRuntimeException(
          String.format("ChangeCapture Query returned a ResultSet that has less than 3 columns. SCN: %s, TS: %s", scn,
              sourceTimestamp));
    }

    // we start from 3 because the first two data fields are SCN and eventTimestamp
    // both of which are only required later for datastreamEvent.metadata
    for (int i = startIndex; i <= colCount; i++) {
      String colName = rsmd.getColumnName(i);
      int colType = rsmd.getColumnType(i);

      String formattedColName = ORACLE_TYPE_INTERPRETER.formatColumn(colName);
      Object result = ORACLE_TYPE_INTERPRETER.sqlObjectToAvro(rs.getObject(i), formattedColName, avroSchema);

      event.addRecord(formattedColName, result, colType);
    }

    return event;
  }
}
