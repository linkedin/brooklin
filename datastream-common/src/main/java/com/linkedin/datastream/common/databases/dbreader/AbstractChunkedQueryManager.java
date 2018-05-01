package com.linkedin.datastream.common.databases.dbreader;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;


public abstract class AbstractChunkedQueryManager implements ChunkedQueryManager {

  @Override
  public void validateQuery(String query) {
  }

  @Override
  public void prepareChunkedQuery(PreparedStatement stmt, List<Object> values) throws SQLException {
    int count = values.size();
    for (int i = 0, index = 1; i < count; i++) {
      for (int j = 0; j <= i; j++, index++) {
        stmt.setObject(index, values.get(j));
      }
    }
  }
}

