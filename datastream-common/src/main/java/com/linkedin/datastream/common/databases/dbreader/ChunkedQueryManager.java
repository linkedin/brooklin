package com.linkedin.datastream.common.databases.dbreader;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;


public interface ChunkedQueryManager {
  /**
   * Validate the nested query to be compatible for chunking
   * @param query
   */
  void validateQuery(String query);

  /**
   * With chunking, the first query cannot ignore any rows and subsequent ones will ignore row previously
   * seen. So we might need a different query the first time.
   *
   * An example Oracle chunked query for an input query "SELECT * FROM table" and keys {k1, k2} will look like
   * <pre>
   *    SELECT * FROM
   *      ( SELECT * FROM table )
   *    WHERE ORA_HASH ( CONCAT ( k1,k2 ) , 10 ) = 3
   *    AND ROWNUM <= 10000
   * where maxChunkIndex is 10, currentIndex is 3 and chunkSize is 10000
   * </pre>
   *
   * @param nestedQuery
   * @param keys
   * @param chunkSize
   * @param maxChunkIndex
   * @param currentIndex
   * @return
   */
  String generateFirstQuery(String nestedQuery, List<String> keys, long chunkSize, long maxChunkIndex, long currentIndex);

  /**
   * Generate the final query after appending the chunking predicate.
   * An example Oracle chunked query for an input query "SELECT * FROM table" and keys {k1, k2} will look like
   * <pre>
   *    SELECT * FROM
   *      ( SELECT * FROM table )
   *    WHERE ORA_HASH ( CONCAT ( k1,k2 ) , 10 ) = 3
   +    AND ( ( k1 > ? ) OR ( k1 = ? AND k2 > ? ) )
   *    AND ROWNUM <= 10000
   * </pre>
   * where maxChunkIndex is 10, currentIndex is 3 and chunkSize is 10000
   *
   * @param nestedQuery The inner query to be wrapped with chunking predicate
   * @param keys Ordered list of keys to be used for generating chunking predicate
   * @param chunkSize Chunk size
   * @param maxchunkIndex Max chunking buckets
   * @param currentIndex Chunk index to match
   * @return
   */
  String generateChunkedQuery(String nestedQuery, List<String> keys, long chunkSize, long maxchunkIndex, long currentIndex);

  /**
   * Set the variables in the preparedStatement query.
   * Assumes query generated from {@code ::generateChunkedQuery()}
   * @param stmt PreparedStatement to populate the variables into
   * @param values List of values to use to initialize the variables in the prepared statement
   * @throws SQLException
   */
  void prepareChunkedQuery(PreparedStatement stmt, List<Object> values) throws SQLException;
}
