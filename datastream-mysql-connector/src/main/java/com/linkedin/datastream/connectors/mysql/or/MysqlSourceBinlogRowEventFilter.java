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

import com.google.code.or.binlog.BinlogEventV4Header;
import com.google.code.or.binlog.BinlogParserContext;
import com.google.code.or.binlog.BinlogRowEventFilter;
import com.google.code.or.binlog.impl.event.TableMapEvent;

public class MysqlSourceBinlogRowEventFilter implements BinlogRowEventFilter {
  private final String _databaseName;
  private final String _tableName;

  public MysqlSourceBinlogRowEventFilter(String databaseName, String tableName) {
    _databaseName = databaseName;
    _tableName = tableName;
  }

  @Override
  public boolean accepts(BinlogEventV4Header header, BinlogParserContext context, TableMapEvent event) {
    if (event.getDatabaseName() == null || event.getDatabaseName() == null) {
      return false;
    }

    if (_databaseName == null) {
      return true;
    }

    if ((event.getDatabaseName().toString()).equalsIgnoreCase(_databaseName)) {
      if (_tableName == null) {
        return true;
      } else {
        return (event.getTableName().toString()).equalsIgnoreCase(_tableName);
      }
    } else {
      return false;
    }
  }
}
