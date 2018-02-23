package com.linkedin.datastream.common.databases;

import java.util.Objects;


/**
 * The DatabaseColumnRecord class is used to represent a specific tuple from a ResultSet
 * The colName represents the column name, and the Value represents the value.
 * Since DatastreamEvents and Database might not share the same types (i.e. CHARS vs Strings),
 * each Record maintains information about the original SQL type
 */
public class DatabaseColumnRecord {
  private final String _colName;
  private final Object _value;
  private final int _sqlType;

  public DatabaseColumnRecord(String colName, Object value, int sqlType) {
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

  public String toString() {
    StringBuilder str = new StringBuilder();
    str.append("[" + _colName + ":");
    str.append(_value == null ? "null" : _value.toString());
    str.append(":" + _sqlType + "]");
    return str.toString();
  }

  public boolean equals(Object o) {
    if (!(o instanceof DatabaseColumnRecord)) {
      return false;
    }
    DatabaseColumnRecord that = (DatabaseColumnRecord) o;
    if (!this.getColName().equals(that.getColName())) {
      return false;
    }
    if (!this.getValue().equals(that.getValue())) {
      return false;
    }
    if (this.getSqlType() != that.getSqlType()) {
      return false;
    }

    return true;
  }

  public int hashCode() {
    return Objects.hash(this.getColName(), this.getValue(), this.getSqlType());
  }
}

