package com.linkedin.datastream.dbreader;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;


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

  /**
   * Assumes query generated from {@code ::generateKeyChunkingPredicate()}
   * @param stmt
   * @param keys
   * @throws SQLException
   */
  public void prepareChunkedQuery(PreparedStatement stmt, Map<String, Object> keys) throws SQLException {
    int numkeys = keys.size();
    Iterator<String> lsbKeys = keys.keySet().iterator();
    stmt.setObject(1, keys.get(lsbKeys.next()));
    int count = 2;
    for (int i = 1; i < numkeys; i++) {
      Iterator<String> msbKeys = keys.keySet().iterator();
      for (int j = 0; j < i; j++) {
        stmt.setObject(count++, keys.get(msbKeys.next()));
      }
      stmt.setObject(count++, keys.get(lsbKeys.next()));
    }
  }
}

