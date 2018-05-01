package com.linkedin.datastream.common.databases.dbreader;

import org.testng.annotations.Test;


public class TestOracleChunkedQueryManager extends TestChunkedQueryManagerBase {
  private static final ChunkedQueryManager MANAGER = new OracleChunkedQueryManager();

  /**
   * Test query string when a single primary KEY is involved in chunking with a single PARTITION assigned.
   */
  @Test
  public void testSimpleKeySinglePartition() {

    /**
     *  SELECT * FROM
     *  (
     *      SELECT * FROM
     *          (
     *              SELECT * FROM TABLE
     *          ) WHERE ( ORA_HASH ( KEY1 , 9 ) = 3 )
     *          ORDER BY KEY1
     *  ) WHERE ROWNUM <= 10
     */

    String firstExpected = "SELECT * FROM ( SELECT * FROM ( SELECT * FROM TABLE ) "
        + "WHERE ( ORA_HASH ( KEY1 , 9 ) = 3 ) ORDER BY KEY1 ) WHERE ROWNUM <= 10";

    /**
     *  SELECT * FROM
     *  (
     *      SELECT * FROM
     *          (
     *              SELECT * FROM TABLE
     *          ) WHERE ( ORA_HASH ( KEY1 , 9 ) = 3 ) AND KEY1 > ?
     *          ORDER BY KEY1
     *  ) WHERE ROWNUM <= 10
     */
    String chunkedExpected =
        "SELECT * FROM ( SELECT * FROM ( SELECT * FROM TABLE ) "
            + "WHERE ( ORA_HASH ( KEY1 , 9 ) = 3 ) AND KEY1 > ? ORDER BY KEY1 ) WHERE ROWNUM <= 10";
    testQueryString(MANAGER, firstExpected, chunkedExpected, NESTED_QUERY, KEY, CHUNK_SIZE, PARTITION_COUNT, PARTITION);
  }

  /**
   * Test query string when a simple KEY is involved in chunking with multiple PARTITION assigned
   */
  @Test
  public void testSimpleKeyMultiPartition() {

    /**
     *   SELECT * FROM
     *   (
     *       SELECT * FROM
     *           (
     *               SELECT * FROM TABLE
     *           ) WHERE ( ORA_HASH ( KEY1 , 9 ) = 2 OR ORA_HASH ( KEY1 , 9 ) = 5 )
     *       ORDER BY KEY1
     *   ) WHERE ROWNUM <= 10
     */
    String firstExpected = "SELECT * FROM ( SELECT * FROM ( SELECT * FROM TABLE ) WHERE ( ORA_HASH ( KEY1 , 9 ) = 2 "
        + "OR ORA_HASH ( KEY1 , 9 ) = 5 ) ORDER BY KEY1 ) WHERE ROWNUM <= 10";

    /**
     *   SELECT * FROM
     *   (
     *       SELECT * FROM
     *           (
     *               SELECT * FROM TABLE
     *           ) WHERE ( ORA_HASH ( KEY1 , 9 ) = 2 OR ORA_HASH ( KEY1 , 9 ) = 5 ) AND KEY1 > ?
     *           ORDER BY KEY1
     *   ) WHERE ROWNUM <= 10
     */
    String chunkedExpected = "SELECT * FROM ( SELECT * FROM ( SELECT * FROM TABLE ) WHERE ( ORA_HASH ( KEY1 , 9 ) = 2 "
        + "OR ORA_HASH ( KEY1 , 9 ) = 5 ) AND KEY1 > ? ORDER BY KEY1 ) WHERE ROWNUM <= 10";
    testQueryString(MANAGER, firstExpected, chunkedExpected, NESTED_QUERY, KEY, CHUNK_SIZE, PARTITION_COUNT,
        PARTITIONS);
  }

  /**
   * Test query string when a composite KEY is involved in chunking with a single PARTITION assigned
   */
  @Test
  public void testCompositeKeySinglePartition() {
    /**
     *    SELECT * FROM
     *    (
     *        SELECT * FROM
     *            (
     *                SELECT * FROM TABLE
     *            ) WHERE ( ORA_HASH ( CONCAT ( KEY1 , KEY2 ) , 9 ) = 3 )
     *        ORDER BY KEY1 , KEY2
     *    ) WHERE ROWNUM <= 10
     */
    String firstExpected = "SELECT * FROM ( SELECT * FROM ( SELECT * FROM TABLE ) "
        + "WHERE ( ORA_HASH ( CONCAT ( KEY1 , KEY2 ) , 9 ) = 3 ) ORDER BY KEY1 , KEY2 ) WHERE ROWNUM <= 10";

    /**
     *  SELECT * FROM
     *  (
     *          SELECT * FROM
     *              (
     *                  SELECT * FROM
     *                      (
     *                          SELECT * FROM
     *                              (
     *                                  SELECT * FROM TABLE
     *                              ) WHERE ( ORA_HASH ( CONCAT ( KEY1 , KEY2 ) , 9 ) = 3 ) AND KEY1 > ?
     *                              ORDER BY KEY1 , KEY2
     *                      ) WHERE ROWNUM <= 10
     *
     *                  UNION ALL
     *
     *                  SELECT * FROM
     *                      (
     *                          SELECT * FROM
     *                              (
     *                                  SELECT * FROM TABLE
     *                              ) WHERE ( ORA_HASH ( CONCAT ( KEY1 , KEY2 ) , 9 ) = 3 ) AND KEY1 = ? AND KEY2 > ?
     *                      ORDER BY KEY1 , KEY2
     *              ) WHERE ROWNUM <= 10
     *      ) ORDER BY KEY1 , KEY2
     *
     *  ) WHERE ROWNUM <= 10
     */
    String chunkedExpected = "SELECT * FROM ( SELECT * FROM ( SELECT * FROM ( SELECT * FROM ( SELECT * FROM TABLE ) "
        + "WHERE ( ORA_HASH ( CONCAT ( KEY1 , KEY2 ) , 9 ) = 3 ) AND KEY1 > ? ORDER BY KEY1 , KEY2 ) "
        + "WHERE ROWNUM <= 10 UNION ALL SELECT * FROM ( SELECT * FROM ( SELECT * FROM TABLE ) "
        + "WHERE ( ORA_HASH ( CONCAT ( KEY1 , KEY2 ) , 9 ) = 3 ) AND KEY1 = ? AND KEY2 > ? ORDER BY KEY1 , KEY2 ) "
        + "WHERE ROWNUM <= 10 ) ORDER BY KEY1 , KEY2 ) WHERE ROWNUM <= 10";
    testQueryString(MANAGER, firstExpected, chunkedExpected, NESTED_QUERY, KEYS, CHUNK_SIZE, PARTITION_COUNT,
        PARTITION);
  }

  /**
   * Test full query string when a composite KEY is involved in chunking with multiple PARTITIONS assigned
   */
  @Test
  public void testCompositeKeyMultiPartition() {

    /**
     *   SELECT * FROM
     *   (
     *       SELECT * FROM
     *           (
     *               SELECT * FROM TABLE
     *           ) WHERE ( ORA_HASH ( CONCAT ( KEY1 , KEY2 ) , 9 ) = 2 OR ORA_HASH ( CONCAT ( KEY1 , KEY2 ) , 9 ) = 5 )
     *       ORDER BY KEY1 , KEY2
     *   ) WHERE ROWNUM <= 10
     */
    String firstExpected = "SELECT * FROM ( SELECT * FROM ( SELECT * FROM TABLE ) "
        + "WHERE ( ORA_HASH ( CONCAT ( KEY1 , KEY2 ) , 9 ) = 2 OR ORA_HASH ( CONCAT ( KEY1 , KEY2 ) , 9 ) = 5 ) "
        + "ORDER BY KEY1 , KEY2 ) WHERE ROWNUM <= 10";

    /**
     *  SELECT * FROM
     *  (
     *      SELECT * FROM
     *      (
     *           SELECT * FROM
     *           (
     *                 SELECT * FROM
     *                 (
     *                       SELECT * FROM TABLE
     *                 ) WHERE ( ORA_HASH ( CONCAT ( KEY1 , KEY2 ) , 9 ) = 2 OR ORA_HASH ( CONCAT ( KEY1 , KEY2 ) , 9 ) = 5 ) AND KEY1 > ?
     *                   ORDER BY KEY1 , KEY2
     *            ) WHERE ROWNUM <= 10
     *
     *            UNION ALL
     *
     *            SELECT * FROM
     *            (
     *                  SELECT * FROM
     *                  (
     *                       SELECT * FROM TABLE
     *                  ) WHERE ( ORA_HASH ( CONCAT ( KEY1 , KEY2 ) , 9 ) = 2 OR ORA_HASH ( CONCAT ( KEY1 , KEY2 ) , 9 ) = 5 ) AND KEY1 = ? AND KEY2 > ?
     *                    ORDER BY KEY1 , KEY2
     *            ) WHERE ROWNUM <= 10
     *       ) ORDER BY KEY1 , KEY2
     *  ) WHERE ROWNUM <= 10
     *
     */

    String chunkedExpected = "SELECT * FROM ( SELECT * FROM ( SELECT * FROM ( SELECT * FROM ( SELECT * FROM TABLE ) "
        + "WHERE ( ORA_HASH ( CONCAT ( KEY1 , KEY2 ) , 9 ) = 2 OR ORA_HASH ( CONCAT ( KEY1 , KEY2 ) , 9 ) = 5 ) "
        + "AND KEY1 > ? ORDER BY KEY1 , KEY2 ) WHERE ROWNUM <= 10 UNION ALL SELECT * FROM ( SELECT * FROM ( SELECT * FROM TABLE ) "
        + "WHERE ( ORA_HASH ( CONCAT ( KEY1 , KEY2 ) , 9 ) = 2 OR ORA_HASH ( CONCAT ( KEY1 , KEY2 ) , 9 ) = 5 ) "
        + "AND KEY1 = ? AND KEY2 > ? ORDER BY KEY1 , KEY2 ) WHERE ROWNUM <= 10 ) ORDER BY KEY1 , KEY2 ) WHERE ROWNUM <= 10";

    testQueryString(MANAGER, firstExpected, chunkedExpected, NESTED_QUERY, KEYS, CHUNK_SIZE, PARTITION_COUNT,
        PARTITIONS);
  }
}
