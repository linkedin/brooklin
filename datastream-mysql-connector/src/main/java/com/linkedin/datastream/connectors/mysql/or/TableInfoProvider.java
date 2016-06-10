package com.linkedin.datastream.connectors.mysql.or;

import java.util.List;


public interface TableInfoProvider {

  List<ColumnInfo> getColumnList(String dbName, String tableName);
}
