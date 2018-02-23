package com.linkedin.datastream.common.databases.dbreader;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;


public abstract class AbstractChunkedQueryManager implements ChunkedQueryManager {

  /**
   * Generate chunking predicate based on keys
   * @param keyList Ordered list of keys to use
   * @return
   */
  public static String generateKeyChunkingPredicate(List<String> keyList) {
    // todo : add a token generator for use with the following query generating APIs.
    // This will allow for writing robust unit-tests.

    StringBuilder str = new StringBuilder();
    int numkeys = keyList.size();
    str.append("(");
    str.append(" ( " + keyList.get(0) + " > ? )");
    for (int i = 1; i < numkeys; i++) {
      str.append(" OR ( ");
      for (int j = 0; j < i; j++) {
        str.append(keyList.get(j) + " = ? AND ");
      }
      str.append(keyList.get(i) + " > ? )");
    }
    str.append(" )");
    return str.toString();
  }

  public void prepareChunkedQuery(PreparedStatement stmt, List<Object> values) throws SQLException {
    int numkeys = values.size();
    stmt.setObject(1, values.get(0));
    for (int count = 2, i = 1; i < numkeys; i++) {
      for (int j = 0; j < i; j++) {
        stmt.setObject(count++, values.get(j));
      }
      stmt.setObject(count++, values.get(i));
    }
  }
}

