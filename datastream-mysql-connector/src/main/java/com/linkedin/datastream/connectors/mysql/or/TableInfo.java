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

import java.util.ArrayList;
import java.util.List;


/** Metadata about a table, its columns and keys. */
public class TableInfo {
  private final List<ColumnInfo> _columns;
  private final List<String> _keyColumns;
  private final List<Integer> _keyColumnIndices;

  public List<ColumnInfo> getColumns() {
    return _columns;
  }

  public List<String> getKeyColumns() {
    return _keyColumns;
  }

  public List<Integer> getKeyColumnIndices() {
    return _keyColumnIndices;
  }

  public TableInfo(List<ColumnInfo> columns, List<String> keyColumns) {
    super();
    this._columns = columns;
    this._keyColumns = keyColumns;
    this._keyColumnIndices = new ArrayList<Integer>();
    for (String k : _keyColumns) {
      _keyColumnIndices.add(getIndex(k));
    }
  }

  public int getIndex(String columnName) {
    int index = -1;
    for (int i = 0; i < _columns.size(); i++) {
      if (columnName.equals(_columns.get(i).getColumnName())) {
        index = i;
        break;
      }
    }
    return index;
  }

  public ColumnInfo getColumn(String columnName) {
    int colIdx = getIndex(columnName);
    if (-1 == colIdx) {
      throw new IllegalArgumentException("unknown column: " + columnName);
    }
    return _columns.get(colIdx);
  }

  @Override
  public String toString() {
    return "TableInfo [_columns=" + _columns + ", _keyColumns=" + _keyColumns + ", _keyColumnIndices="
        + _keyColumnIndices + "]";
  }
}
