package com.linkedin.datastream.connectors.mysql.or;

import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class InMemoryTableInfoProvider implements TableInfoProvider {
  private final Map<String, List<ColumnInfo>> _tableInfo = new HashMap<>();

  private static final String FULL_TABLENAME_FORMAT = "%s.%s";

  private static InMemoryTableInfoProvider _tableInfoProvider = new InMemoryTableInfoProvider();

  public static InMemoryTableInfoProvider getTableInfoProvider() {
    return _tableInfoProvider;
  }

  private InMemoryTableInfoProvider() {
  }

  public void addTableInfo(String dbName, String tableName, List<ColumnInfo> tableInfo) {
    _tableInfo.put(String.format(FULL_TABLENAME_FORMAT, dbName, tableName), tableInfo);
  }

  @Override
  public List<ColumnInfo> getColumnList(String dbName, String tableName) {
    return _tableInfo.get(String.format(FULL_TABLENAME_FORMAT, dbName, tableName));
  }
}
