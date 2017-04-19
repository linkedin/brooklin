package com.linkedin.datastream.connectors.oracle.triggerbased.consumer;

import java.util.ArrayList;
import java.util.List;


/**
 * The OracleChangeEvent class is to help represent the result Set returned
 * from the change queries
 *
 * as a comparison to a traditional SQL table:
 *  OracleChangeEvent.Record represents the individual tuples in a row
 *  OracleChangeEvent represents a complete Row. (built by x number of OracleChangeEvent.Records)
 *  List<OracleChangeEvent> represents a full table.
 */
public class OracleChangeEvent {
  private List<Record> _records = new ArrayList<>();
  private final long _scn;
  private final long _sourceTimestamp;

  public OracleChangeEvent(long scn, long ts) {
    _scn = scn;
    _sourceTimestamp = ts;
  }

  public void addRecord(String colName, Object val, int sqlType) {
    Record record = new Record(colName, val, sqlType);
    _records.add(record);
  }

  public List<Record> getRecords() {
    return _records;
  }

  public int size() {
    return _records.size();
  }

  public long getScn() {
    return _scn;
  }

  public long getSourceTimestamp() {
    return _sourceTimestamp;
  }

  /**
   * The Record class is used to represent a specific tuple from a ResultSet
   * The colName represents the column name, and the Value represents the value.
   * Since DatastreamEvents and Oracle dont share the same types (i.e. CHARS vs Strings),
   * each Record maintains information about the original SQL type
   */
  public final class Record {

    private final String _colName;
    private final Object _value;
    private final int _sqlType;

    public Record(String colName, Object value, int sqlType) {
      _colName = colName;
      _value = value;
      _sqlType = sqlType;
    }

    public String getColName() {
      return _colName;
    }

    public Object getValue() {
      return _value;
    }

    public int getSqlType() {
      return _sqlType;
    }
  }
}