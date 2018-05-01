package com.linkedin.datastream.common.databases.dbreader;

import java.util.List;

import org.apache.commons.lang.Validate;


/**
 * Query manager for Mysql
 */
public class MySqlChunkedQueryManager extends AbstractChunkedQueryManager {
  private static final String SELECT_FROM = "SELECT * FROM ( ";

  /** Generate base predicate for shariding keys to given number of partitions.
   *  Ex: MOD ( MD5 ( CONCAT ( K1, K2, K3 ) ) , 10 ) for a table with 3 keys {K1, K2, K3} and 10 partitions */
  private String generatePerPartitionHashPredicate(List<String> keys, long partitionCount) {
    StringBuilder query = new StringBuilder();
    int keyCount = keys.size();
    // Mysql CONCAT is not a binary operator and can take any number of arguments, including a single argument
    // CONCAT (A, B, C)
    query.append("CONCAT ( ").append(keys.get(0));
    for (int i = 1; i < keyCount; i++) {
      query.append(" , ").append(keys.get(i));
    }
    query.append(" )");

    // Wrap that with MOD and MD5 to generate a hash for sharding
    // MOD ( MD5 ( CONCAT ( A, B, C ) ) , 10 )
    query.insert(0, "MD5 ( ").append(" )");
    query.insert(0, "MOD ( ").append(" , ").append(partitionCount).append(" )");
    return query.toString();
  }

  /** Generate predicate for filtering rows hashing to the assigned partitions :
   *  Ex: WHERE ( MOD ( MD5 ( CONCAT ( K1, K2, K3 ) ) , 10 ) = 1 OR MOD ( MD5 ( CONCAT ( K1, K2, K3 ) ) , 10 ) = 6 )
   *  where 1 and 6 are the assigned partitions, 10 the partition count and, {K1, K2, K3} the keys of the table
   */
  private String generateFullPartitionHashPredicate(String perPartitionPredicate, List<Integer> partitions) {
    StringBuilder query = new StringBuilder();
    int numPartitions = partitions.size();

    query.append("WHERE ( ").append(perPartitionPredicate).append(" = ").append(partitions.get(0));
    for (int i = 1; i < numPartitions; i++) {
      query.append(" OR ").append(perPartitionPredicate).append(" = ").append(partitions.get(i));
    }
    query.append(" )");

    return query.toString();
  }

  private String generateKeyChunkingPredicate(List<String> keys) {
    StringBuilder str = new StringBuilder();
    int numkeys = keys.size();
    str.append("( " + keys.get(0) + " > ? )");
    for (int i = 1; i < numkeys; i++) {
      str.append(" OR ( ");
      for (int j = 0; j < i; j++) {
        str.append(keys.get(j) + " = ? AND ");
      }
      str.append(keys.get(i) + " > ? )");
    }
    return str.toString();
  }

  private String generateChunkedQuery(String nestedQuery, List<String> keys, long chunkSize, int partitionCount,
      List<Integer> partitions, boolean isFirstRun) {
    Validate.isTrue(!keys.isEmpty(), "Need keys to generate chunked query. No keys supplied");

    String hashPredicate = generateFullPartitionHashPredicate(generatePerPartitionHashPredicate(keys, partitionCount), partitions);

    StringBuilder query = new StringBuilder();
    query.append(SELECT_FROM);
    query.append(nestedQuery);
    // 'nestedTab' alias needed as mysql requires every derived table to have its own alias
    query.append(" ) nestedTab1 ");
    query.append(hashPredicate);
    if (!isFirstRun) {
      query.append(" AND ( " + generateKeyChunkingPredicate(keys)).append(" )");
    }

    query.append(" ORDER BY ").append(keys.get(0));
    for (int i = 1; i < keys.size(); i++) {
      query.append(" , ").append(keys.get(i));
    }

    query.insert(0, SELECT_FROM).append(" ) as nestedTab2 LIMIT " + chunkSize);
    return query.toString();
  }

  @Override
  public String generateFirstQuery(String nestedQuery, List<String> keys, long chunkSize, int partitionCount,
      List<Integer> partitions) {
    return generateChunkedQuery(nestedQuery, keys, chunkSize, partitionCount, partitions, true);
  }

  @Override
  public String generateChunkedQuery(String nestedQuery, List<String> keys, long chunkSize, int partitionCount,
      List<Integer> partitions) {
    return generateChunkedQuery(nestedQuery, keys, chunkSize, partitionCount, partitions, false);
  }
}
