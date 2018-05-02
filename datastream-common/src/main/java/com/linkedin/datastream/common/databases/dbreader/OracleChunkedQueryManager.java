package com.linkedin.datastream.common.databases.dbreader;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import org.apache.commons.lang.Validate;


/**
 * Oracle chunked query manager.
 */
public class OracleChunkedQueryManager implements ChunkedQueryManager {
  private static final String SELECT_FROM = "SELECT * FROM ( ";

  /**
   * Generate predicate for filtering rows hashing to the assigned partitions.
   * Ex:
   * <pre>
   * SELECT * FROM ( nestedQuery )
   * WHERE ( ORA_HASH ( CONCAT ( CONCAT ( MEMBER_ID , ANET_ID ) , SETTING_ID ) , 9 ) = 1
   *         OR
   *         ORA_HASH ( CONCAT ( CONCAT ( MEMBER_ID , ANET_ID ) , SETTING_ID ) , 9 ) = 2
   *       )
   * </pre>
   * using the base partitioning predicate
   * <pre>
   *   ORA_HASH ( CONCAT ( CONCAT ( MEMBER_ID , ANET_ID ) , SETTING_ID ) , 9 )
   * </pre>
   */
  private static String generateFullHashPredicate(String nestedQuery, String perPartitionHashPredicate,
      List<Integer> partitions) {
    StringBuilder fullHashPredicate = new StringBuilder("SELECT * FROM ( " + nestedQuery + " ) WHERE ( ");
    int numPartitions = partitions.size();
    fullHashPredicate.append(perPartitionHashPredicate).append(" IN ( ").append(partitions.get(0));
    for (int i = 1; i < numPartitions; i++) {
      fullHashPredicate.append(" , ").append(partitions.get(i));
    }
    fullHashPredicate.append(" ) )");
    return fullHashPredicate.toString();
  }

  /**
   * Generates the predicate that will be used to shard keys into partitions. Example:
   * ( ORA_HASH ( CONCAT ( CONCAT ( MEMBER_ID , ANET_ID ) , SETTING_ID ) , 9 ) where
   * partition count = 10. The <i>max_bucket</i> value in ORA_HASH(expr [, max_bucket ]) = PARTITION_NUMBER
   * is the max value returned by the hash function, so values run from 0 to partitionCount - 1.
   */
  private static String generatePerPartitionHash(List<String> keys, int partitionCount) {
    int keyCount = keys.size();
    StringBuilder perPartitionHashPredicate = new StringBuilder();

    if (keyCount == 1) {
      perPartitionHashPredicate.append(keys.get(0));
    } else {
      perPartitionHashPredicate.append("CONCAT ( ");
      perPartitionHashPredicate.append(keys.get(0));
      perPartitionHashPredicate.append(" , ");
      perPartitionHashPredicate.append(keys.get(1));
      perPartitionHashPredicate.append(" )");

      for (int i = 2; i < keyCount; i++) {
        perPartitionHashPredicate.insert(0, "CONCAT ( ");
        perPartitionHashPredicate.append(" , ");
        perPartitionHashPredicate.append(keys.get(i));
        perPartitionHashPredicate.append(" )");
      }
    }

    perPartitionHashPredicate.insert(0, "ORA_HASH ( ");

    perPartitionHashPredicate.append(" , " + (partitionCount - 1) + " )");

    return perPartitionHashPredicate.toString();
  }

  private static String generateOrderByClause(List<String> keys) {
    StringBuilder clause = new StringBuilder();
    clause.append("ORDER BY ");
    clause.append(keys.get(0));
    for (int i = 1; i < keys.size(); i++) {
      clause.append(" , " + keys.get(i));
    }
    return clause.toString();
  }

  /** Generate the first paginated query. No rows can be ignored, so only has hashing predicate for filtering row with
   *  keys hashing to assigned partitions.
   *  <pre>
   *    SELECT * FROM
   *    (
   *       SELECT * FROM
   *       (
   *         SELECT * FROM ANET_MEMBERS
   *       ) WHERE (ORA_HASH ( CONCAT ( MEMBER_ID , ANET_ID ) , 9 ) = 2 OR ORA_HASH ( CONCAT ( MEMBER_ID , ANET_ID ) , 9 ) = 5 )
   *         ORDER BY MEMBER_ID , ANET_ID
   *    ) WHERE ROWNUM <= 10
   *  </pre>
   * @param nestedQuery Query for the table/view
   * @param keys Unique keys to use for chunking
   * @param chunkSize Rows to read pr query
   * @param partitionCount Partition count
   * @param partitions Partitions assigned from sharding
   * @return Query
   */
  @Override
   public String generateFirstQuery(String nestedQuery, List<String> keys, long chunkSize, int partitionCount,
      List<Integer> partitions) {
    Validate.isTrue(!keys.isEmpty(), "Need keys to generate chunked query. No keys supplied");

    String orderedPartitionedQuery =
        generateFullHashPredicate(nestedQuery, generatePerPartitionHash(keys, partitionCount), partitions);

    StringBuilder innerQuery = new StringBuilder(orderedPartitionedQuery);
    innerQuery.append(" " + generateOrderByClause(keys));


    // wrap this in a ROWNUM constraint of its own since ROWNUM is applied before ORDER-by if used together in one constraint
    innerQuery.insert(0, SELECT_FROM).append(" ) WHERE ROWNUM <= ").append(chunkSize);
    return innerQuery.toString();
  }

  /**
   * For two keys K1 and K2 with last row from previous query having values V1 and V2, the predicate to ignore rows
   * previously seen could have been <b>WHERE (( k1 > V1 ) OR ( k1 = V1 AND k2 > V2 ) )</b>
   * However the use of the OR clause leads to Oracle query optimizer not using RANGE-INDEX scan and instead
   * ends up doing a full index scan leading to o(n^2) read complexity.
   * To make use of RANGE-INDEX scan optimally, the query can be transformed into separate queries handling each
   * part of the OR and a union of all the individual queries. The union however does require the result be sorted again,
   * and the overall complexity is O(KC lg C) where C is the chunk size and K = n/c, the number of queries done.
   * O(KC lg C) = O(n lg C) and since C is typically very small compared to n, it still gives far better complexity
   * compared to the non-range index scan approach.
   */
  private static String generateChunkedQueryHelper(String nestedQuery, List<String> keys, long chunkCount) {
    StringBuilder query = new StringBuilder();

    // for each key, the query will progressively add a = previous key parts and > this key part
    int keyCount = keys.size();
    for (int i = 0; i < keyCount; i++) {
      if (i != 0) {
        query.append(" UNION ALL ");
      }

      StringBuilder subQuery = new StringBuilder(nestedQuery);
      int j = 0;
      for (; j < i; j++) {
        subQuery.append(" AND ").append(keys.get(j)).append(" = ?");
      }

      // last key for this sub query, so ignore anything less than
      subQuery.append(" AND ").append(keys.get(j)).append(" > ?");

      subQuery.append(" " + generateOrderByClause(keys));

      // ROWNUM needs to be added to its own SELECT * and cannot go with ORDER-BY clause sine ROWNUM gets applied before
      // order by
      subQuery.insert(0, SELECT_FROM).append(" ) WHERE ROWNUM <= ").append(chunkCount);

      // append the subquery to main query
      query.append(subQuery);
    }

    // The outermost query should have a ROWNUM and ORDER-By constraint of its own if if had more than 1 key
    if (keyCount > 1) {
      query.insert(0, SELECT_FROM).append(" ) " + generateOrderByClause(keys));
      query.insert(0, SELECT_FROM).append(" ) WHERE ROWNUM <= ").append(chunkCount);
    }

    return query.toString();
  }

  /**
   * Generate chunked query which ignores previously seen rows. An example query will look like this:
   *  SELECT * FROM
   *  (
   *    SELECT * FROM
   *        (
   *            SELECT * FROM
   *                (
   *                    SELECT * FROM
   *                        (
   *                            SELECT * FROM TABLE
   *                        ) WHERE ( ORA_HASH ( CONCAT ( KEY1 , KEY2 ) , 9 ) = 3 ) AND KEY1 > ?
   *                        ORDER BY KEY1 , KEY2
   *                ) WHERE ROWNUM <= 10
   *
   *            UNION ALL
   *
   *            SELECT * FROM
   *                (
   *                    SELECT * FROM
   *                        (
   *                            SELECT * FROM TABLE
   *                        ) WHERE ( ORA_HASH ( CONCAT ( KEY1 , KEY2 ) , 9 ) = 3 ) AND KEY1 = ? AND KEY2 > ?
   *                ORDER BY KEY1 , KEY2
   *        ) WHERE ROWNUM <= 10
   *      ) ORDER BY KEY1 , KEY2
   *
   *   ) WHERE ROWNUM <= 10
   */
  @Override
  public String generateChunkedQuery(String nestedQuery, List<String> keys, long chunkSize, int partitionCount,
      List<Integer> partitions) {
    Validate.isTrue(!keys.isEmpty(), "Need keys to generate chunked query. No keys supplied");

    String shardedQuery =
        generateFullHashPredicate(nestedQuery, generatePerPartitionHash(keys, partitionCount), partitions);
    StringBuilder innerQuery = new StringBuilder().append(shardedQuery);

    // Add predicate to ignore previously seen rows.
    return generateChunkedQueryHelper(innerQuery.toString(), keys, chunkSize);
  }

  @Override
  public void prepareChunkedQuery(PreparedStatement stmt, List<Object> values) throws SQLException {
    int count = values.size();

    // Bind all variables in the PreparedStatement i.e. the '?' to values supplied in the list.

    // For the example query
    //
    //  SELECT * FROM
    //  (
    //    SELECT * FROM
    //        (
    //            SELECT * FROM
    //                (
    //                    SELECT * FROM
    //                        (
    //                            SELECT * FROM TABLE
    //                        ) WHERE ( ORA_HASH ( CONCAT ( KEY1 , KEY2 ) , 9 ) = 3 ) AND KEY1 > ?
    //                        ORDER BY KEY1 , KEY2
    //                ) WHERE ROWNUM <= 10
    //            UNION ALL
    //            SELECT * FROM
    //                (
    //                    SELECT * FROM
    //                        (
    //                            SELECT * FROM TABLE
    //                        ) WHERE ( ORA_HASH ( CONCAT ( KEY1 , KEY2 ) , 9 ) = 3 ) AND KEY1 = ? AND KEY2 > ?
    //                ORDER BY KEY1 , KEY2
    //        ) WHERE ROWNUM <= 10
    //      ) ORDER BY KEY1 , KEY2
    //   ) WHERE ROWNUM <= 10
    // the value for Key1 and Key2 needs to be plugged in order. The index values for PreparedStatement.setObject start
    // at 1.
    for (int i = 0, index = 1; i < count; i++) {
      for (int j = 0; j <= i; j++, index++) {
        stmt.setObject(index, values.get(j));
      }
    }
  }
}
