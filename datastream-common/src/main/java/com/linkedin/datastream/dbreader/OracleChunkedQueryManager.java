package com.linkedin.datastream.dbreader;

import java.util.List;

import com.linkedin.datastream.common.DatastreamRuntimeException;


/**
 * Oracle chunked query manager.
 */
public class OracleChunkedQueryManager extends AbstractChunkedQueryManager {

  /**
   * Throws DatastreamRuntimeException if invalid query.
   * Query is invalid if
   * 1. It has a join
   * 2. All the primary keys are not part of the selected columns
   * 3. If there is no ORDER BY clause or if they are not ordered in the same order as a unique index.
   * 4. If the table in the inner query doesnt match the table supplied in the reader parameter
   * @param query
   */
  public void validateQuery(String query) {
    // todo : Implement this. Is there a better way than string parsing?
  }

  private String generateChunkedQuery(String nestedQuery, List<String> keys, long chunkSize,
      long maxBuckets, long currentIndex, boolean isFirstRun) {
    if (keys.isEmpty()) {
      throw new DatastreamRuntimeException("Need keys to generate chunked query. No keys supplied");
    }

    int keyCount = keys.size();
    StringBuilder tmp = new StringBuilder();
    tmp.append(keys.get(0));
    for (int i = 1; i < keyCount; i++) {
      tmp.append("," + keys.get(i));
    }
    String pkeyString = tmp.toString();

    StringBuilder query = new StringBuilder();
    query.append("SELECT * FROM ( ");
    query.append(nestedQuery);
    query.append(" ) ");
    query.append("WHERE ORA_HASH ( ");

    if (keyCount > 1) {
      query.append("CONCAT ( " + pkeyString + " )");
    } else {
      query.append(pkeyString);
    }

    query.append(" , " + maxBuckets + " ) = " + currentIndex);
    if (!isFirstRun) {
      query.append(" AND " + generateKeyChunkingPredicate(keys));
    }

    query.append(" AND ROWNUM <= " + chunkSize);
    return query.toString();
  }

  public String generateFirstQuery(String nestedQuery, List<String> keys, long chunkSize,
      long maxBuckets, long currentIndex) {
    return generateChunkedQuery(nestedQuery, keys, chunkSize, maxBuckets, currentIndex, true);
  }

  @Override
  public String generateChunkedQuery(String nestedQuery, List<String> keys, long chunkSize, long maxBuckets,
      long currentIndex) {
    return generateChunkedQuery(nestedQuery, keys, chunkSize, maxBuckets, currentIndex, false);
  }
}
