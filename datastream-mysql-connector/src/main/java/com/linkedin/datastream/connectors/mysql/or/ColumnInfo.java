package com.linkedin.datastream.connectors.mysql.or;

/** Metadata about a table column */
public class ColumnInfo {
  private final String _columnName;
  private final String _dataType;
  private final Long _charMaxLength;
  private final Long _charOctetLength;
  private final Long _numericPrecision;
  private final Long _numericScale;
  private final boolean _isKey;

  public String getColumnName() {
    return _columnName;
  }

  public String getDataType() {
    return _dataType;
  }

  public Long getCharMaxLength() {
    return _charMaxLength;
  }

  public Long getCharOctetLength() {
    return _charOctetLength;
  }

  public Long getNumericPrecision() {
    return _numericPrecision;
  }

  public Long getNumericScale() {
    return _numericScale;
  }

  public ColumnInfo(String columnName, boolean isKey, String dataType, Long charMaxLength, Long charOctetLength,
      Long numericPrecision, Long numericScale) {
    super();
    _columnName = columnName;
    _isKey = isKey;
    _dataType = dataType;
    _charMaxLength = charMaxLength;
    _charOctetLength = charOctetLength;
    _numericPrecision = numericPrecision;
    _numericScale = numericScale;
  }

  public ColumnInfo(String columnName, boolean isKey) {
    this(columnName, isKey, "", null, null, null, null);
  }

  public ColumnInfo(String columnName) {
    this(columnName, false, "", null, null, null, null);
  }

  @Override
  public String toString() {
    return "ColumnInfo [columnName=" + _columnName + ", dataType=" + _dataType + ", charMaxLength=" + _charMaxLength
        + ", charOctetLength=" + _charOctetLength + ", numericPrecision=" + _numericPrecision + ", numericScale="
        + _numericScale + "]";
  }

  public boolean isKey() {
    return _isKey;
  }
}
