package com.linkedin.datastream.connectors.mysql.or;

import com.google.code.or.binlog.BinlogEventV4Header;
import com.google.code.or.binlog.BinlogParserContext;
import com.google.code.or.binlog.BinlogRowEventFilter;
import com.google.code.or.binlog.impl.event.TableMapEvent;


public class MysqlSourceBinlogRowEventFilter implements BinlogRowEventFilter {
  private final String _databaseName;
  private final String _tableName;
  private final Boolean _acceptAllTables;

  public MysqlSourceBinlogRowEventFilter(String databaseName, Boolean acceptAllTables, String tableName) {
    _databaseName = databaseName;
    _tableName = tableName;
    _acceptAllTables = acceptAllTables;
  }

  @Override
  public boolean accepts(BinlogEventV4Header header, BinlogParserContext context, TableMapEvent event) {
    if (event.getDatabaseName() == null || event.getTableName() == null) {
      return false;
    }

    if (_databaseName == null) {
      return true;
    }

    if ((event.getDatabaseName().toString()).equalsIgnoreCase(_databaseName)) {
      if (_acceptAllTables) {
        return true;
      } else {
        return (event.getTableName().toString()).equalsIgnoreCase(_tableName);
      }
    } else {
      return false;
    }
  }
}
