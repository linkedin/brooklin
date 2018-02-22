package com.linkedin.datastream.dbreader;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;


public interface ChunkedQueryManager {
  /**
   * Validate the nested query to be compatible for chunking
   * @param query
   */
  void validateQuery(String query);

  /**
   * With chunking, the first query cannot ignore any rows and subsequent ones will ignore row previously
   * seen. So we might need a different query the first time.
   * @param nestedQuery
   * @param keys
   * @param chunkSize
   * @param maxChunkIndex
   * @param currentIndex
   * @return
   */
  String generateFirstQuery(String nestedQuery, List<String> keys, long chunkSize, long maxChunkIndex, long currentIndex);

  /**
   *
   * @param nestedQuery
   * @param keys
   * @param chunkSize
   * @param maxBuckets
   * @param currentIndex
   * @return
   */
  String generateChunkedQuery(String nestedQuery, List<String> keys, long chunkSize, long maxBuckets, long currentIndex);

  /**
   *
   * @param stmt
   * @param keys
   * @throws SQLException
   */
  void prepareChunkedQuery(PreparedStatement stmt, Map<String, Object> keys) throws SQLException;
}
