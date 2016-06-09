package com.linkedin.datastream.connectors.mysql.or;

/*
 * Copyright 2015 LinkedIn Corp. All rights reserved
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/** Metadata about a table column */
public class ColumnInfo {
  private final String _columnName;
  private final String _dataType;
  private final Long _charMaxLength;
  private final Long _charOctetLength;
  private final Long _numericPrecision;
  private final Long _numericScale;

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

  public ColumnInfo(String columnName, String dataType, Long charMaxLength, Long charOctetLength, Long numericPrecision,
      Long numericScale) {
    super();
    this._columnName = columnName;
    this._dataType = dataType;
    this._charMaxLength = charMaxLength;
    this._charOctetLength = charOctetLength;
    this._numericPrecision = numericPrecision;
    this._numericScale = numericScale;
  }

  @Override
  public String toString() {
    return "ColumnInfo [columnName=" + _columnName + ", dataType=" + _dataType + ", charMaxLength=" + _charMaxLength
        + ", charOctetLength=" + _charOctetLength + ", numericPrecision=" + _numericPrecision + ", numericScale="
        + _numericScale + "]";
  }
}
