package com.linkedin.datastream.connectors.oracle.triggerbased.consumer;

import java.util.List;

import com.linkedin.datastream.common.DatabaseColumnRecord;
import com.linkedin.datastream.common.DatabaseRow;

/**
 * The OracleChangeEvent class is to help represent the result Set returned
 * from the change queries
 *
 * as a comparison to a traditional SQL table:
 *  DatabaseRow represents the individual tuples in a row after the change.
 *  OracleChangeEvent is the after image along with the scn and the timestamp of change
 *  List<OracleChangeEvent> represents a full table.
 */
public class OracleChangeEvent {
  private DatabaseRow _row;
  private final long _scn;
  private final long _sourceTimestamp;

  public OracleChangeEvent(long scn, long ts) {
    _scn = scn;
    _sourceTimestamp = ts;
    _row = new DatabaseRow();
  }

  public void addRecord(String colName, Object val, int sqlType) {
    _row.addField(colName, val, sqlType);
  }

  public List<DatabaseColumnRecord> getRecords() {
    return _row.getRecords();
  }

  public int size() {
    return _row.getColumnCount();
  }

  public long getScn() {
    return _scn;
  }

  public long getSourceTimestamp() {
    return _sourceTimestamp;
  }
}