package com.linkedin.datastream.connectors.mysql.or;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;

import com.linkedin.datastream.connectors.mysql.MysqlSource;


public class MysqlServerTableInfoProvider implements TableInfoProvider {

  private final MysqlQueryUtils _queryUtils;
  private HashMap<String, List<ColumnInfo>> _columnMetadataCache;

  public MysqlServerTableInfoProvider(MysqlSource source, String userName, String password) throws SQLException {
    _queryUtils = new MysqlQueryUtils(source, userName, password);
    _queryUtils.initializeConnection();
  }

  public List<ColumnInfo> getColumnList(String dbName, String tableName) {
    // TODO We probably need to expire this cache regularly.
    if (!_columnMetadataCache.containsKey(tableName)) {
      try {

        List<ColumnInfo> columnMetadata = _queryUtils.getColumnList(dbName, tableName);
        _columnMetadataCache.put(tableName, columnMetadata);
        return columnMetadata;
      } catch (SQLException e) {
        //TODO retry?
        throw new RuntimeException("Querying mysql threw an exception", e);
      }
    } else {
      return _columnMetadataCache.get(tableName);
    }
  }
}
