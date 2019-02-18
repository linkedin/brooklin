/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.common.databases.dbreader;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;


public interface ChunkedQueryManager {
  /**
   * Validate the nested query to be compatible for chunking.
   * Throws IllegalArgumentException
   * @param query
   */
  default void validateQuery(String query) throws IllegalArgumentException {

  }

  /**
   * With chunking, the first query cannot ignore any rows and subsequent ones will ignore row previously
   * seen. So we need a different query the first time.
   * @param nestedQuery Query to filter columns
   * @param keys Primary keys to use for chunking rows
   * @param chunkSize Number of rows to chunk in a query
   * @param partitionCount Total partitions that the keys are hashed into
   * @param partitions Partitions that this query should return keys for
   * @return First chunked query to the database
   */
  String generateFirstQuery(String nestedQuery, List<String> keys, long chunkSize, int partitionCount,
      List<Integer> partitions);
  /**
   * Generate the final query after appending the chunking predicate.
   * @param nestedQuery Query to filter columns
   * @param keys Primary keys to use for chunking rows
   * @param chunkSize Number of rows to chunk in a query
   * @param partitionCount Total partitions that the keys are hashed into
   * @param partitions Partitions that this query should return keys for
   * @return Chunked query to the database which ignores previously seen rows
   */
  String generateChunkedQuery(String nestedQuery, List<String> keys, long chunkSize, int partitionCount,
      List<Integer> partitions);

  /**
   * Set the variables in the preparedStatement query.
   * Assumes query generated from {@code ::generateChunkedQuery()}
   * @param stmt PreparedStatement to populate the variables into
   * @param values List of values to use to initialize the variables in the prepared statement
   * @throws SQLException
   */
  void prepareChunkedQuery(PreparedStatement stmt, List<Object> values) throws SQLException;
}
