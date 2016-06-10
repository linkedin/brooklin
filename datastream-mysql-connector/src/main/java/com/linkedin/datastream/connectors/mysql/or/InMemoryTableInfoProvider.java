package com.linkedin.datastream.connectors.mysql.or;

import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class InMemoryTableInfoProvider implements TableInfoProvider {
  private final Map<String, List<ColumnInfo>> _tableInfo = new HashMap<>();

  private static InMemoryTableInfoProvider _tableInfoProvider = new InMemoryTableInfoProvider();

  public static InMemoryTableInfoProvider getTableInfoProvider() {
    return _tableInfoProvider;
  }

  private InMemoryTableInfoProvider() {
  }

  public void addTableInfo(String tableName, List<ColumnInfo> tableInfo) {
    _tableInfo.put(tableName, tableInfo);
  }

  @Override
  public List<ColumnInfo> getColumnList(String dbName, String tableName) {
    return _tableInfo.get(tableName);
  }
}
