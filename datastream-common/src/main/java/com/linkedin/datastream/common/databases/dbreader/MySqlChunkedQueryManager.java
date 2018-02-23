package com.linkedin.datastream.common.databases.dbreader;

import java.util.List;

import com.linkedin.datastream.common.DatastreamRuntimeException;


/**
 * Example query manager for Mysql. Some functions like the hash function might need more testing before use.
 */
public class MySqlChunkedQueryManager extends AbstractChunkedQueryManager {

  @Override
  public void validateQuery(String query) {
  }

  private String generateChunkedQuery(String nestedQuery, List<String> keys, long chunkSize, long maxBuckets,
      long currentIndex, boolean isFirstRun) {
    if (keys.isEmpty()) {
      throw new DatastreamRuntimeException("Need keys to generate chunked query. No keys supplied");
    }

    int keyCount = keys.size();
    StringBuilder tmp = new StringBuilder();
    tmp.append(keys.get(0));
    for (int i = 1; i < keyCount; i++) {
      tmp.append(" || " + keys.get(i));
    }
    String pkeyString = tmp.toString();

    StringBuilder query = new StringBuilder();
    query.append("SELECT * FROM ( ");
    query.append(nestedQuery);
    query.append(" ) nestedTab "); // 'nestedTab' alias needed as mysql requires every derived table to have its own alias
    query.append("WHERE MOD ( MD5 ( "); // NOTE: md5 might not work on all DataTypes. Needs verification on key before use
    query.append(pkeyString);
    query.append(" ) , " + maxBuckets + " ) = " + currentIndex);
    if (!isFirstRun) {
      query.append(" AND " + generateKeyChunkingPredicate(keys));
    }
    query.append(" LIMIT " + chunkSize);
    return query.toString();
  }

  @Override
  public String generateFirstQuery(String nestedQuery, List<String> keys, long chunkSize, long maxChunkIndex,
      long currentIndex) {
    return generateChunkedQuery(nestedQuery, keys, chunkSize, maxChunkIndex, currentIndex, true);
  }

  @Override
  public String generateChunkedQuery(String nestedQuery, List<String> keys, long chunkSize, long maxChunkIndex, long currentIndex) {
    return generateChunkedQuery(nestedQuery, keys, chunkSize, maxChunkIndex, currentIndex, false);
  }
}
