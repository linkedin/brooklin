package com.linkedin.datastream.common.databases.dbreader;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import junit.framework.Assert;


public class TestChunkedQueryManager {

  @BeforeMethod
  public void setup(Method method) {
  }


  /**
   * Test query string when a single primary key is involved in chunking
   */
  @Test
  public void testOracleQueryStringSimpleKey() {
    String nestedQuery = "SELECT * FROM table";
    List<String> keys = Arrays.asList("PKEY");
    long chunkSize = 10000;
    long bucketSize = 10;
    long chunkIndex = 3;

    String expected = "SELECT * FROM ( SELECT * FROM table ) WHERE ORA_HASH ( PKEY , 10 ) = 3 AND ROWNUM <= 10000";

    String actual =
        new OracleChunkedQueryManager().generateFirstQuery(nestedQuery, keys, chunkSize, bucketSize, chunkIndex);

    Assert.assertEquals(expected, actual);

    actual =
        new OracleChunkedQueryManager().generateChunkedQuery(nestedQuery, keys, chunkSize, bucketSize, chunkIndex);

    expected =
        "SELECT * FROM ( SELECT * FROM table ) WHERE ORA_HASH ( PKEY , 10 ) = 3 AND ( ( PKEY > ? ) ) AND ROWNUM <= 10000";
    Assert.assertEquals(expected, actual);
  }

  /**
   * Test query string when a single primary key is involved in chunking
   */
  @Test
  public void testMysqlQueryStringSimpleKey() {
    String nestedQuery = "SELECT * FROM table";
    List<String> keys = Arrays.asList("PKEY");
    long chunkSize = 10000;
    long bucketSize = 10;
    long chunkIndex = 3;

    //String expected = "SELECT * FROM ( SELECT * FROM table ) WHERE ORA_HASH ( PKEY , 10 ) = 3 AND ROWNUM <= 10000";

    String actual =
        new MySqlChunkedQueryManager().generateFirstQuery(nestedQuery, keys, chunkSize, bucketSize, chunkIndex);

    String expected =
        "SELECT * FROM ( SELECT * FROM table ) nestedTab WHERE MOD ( MD5 ( PKEY ) , 10 ) = 3 LIMIT 10000";
    Assert.assertEquals(expected, actual);

    actual =
        new MySqlChunkedQueryManager().generateChunkedQuery(nestedQuery, keys, chunkSize, bucketSize, chunkIndex);
    expected =
        "SELECT * FROM ( SELECT * FROM table ) nestedTab WHERE MOD ( MD5 ( PKEY ) , 10 ) = 3 AND ( ( PKEY > ? ) ) LIMIT 10000";
    Assert.assertEquals(expected, actual);
  }


  /**
   * Test full query string when a composite key is involved in chunking. This is a weak test and needs to be updated
   * once the code starts using a better token generator to get over issues with spaces. Stripping spaces might be
   * would be fine, except query without the spaces is not a valid query and so it is better to have a stricter test
   * until the token generator is added.
   */
  @Test
  public void testQueryStringCompositeKey() {
    String nestedQuery = "SELECT * FROM table";
    List<String> keys = Arrays.asList("PKEY1", "PKEY2");
    long chunkSize = 10000;
    long bucketSize = 10;
    long chunkIndex = 3;

    String actual =
        new OracleChunkedQueryManager().generateChunkedQuery(nestedQuery, keys, chunkSize, bucketSize, chunkIndex);
    String expected = "SELECT * FROM ( SELECT * FROM table ) WHERE ORA_HASH ( CONCAT ( PKEY1,PKEY2 ) , 10 ) = 3 AND "
        + "( ( PKEY1 > ? ) OR ( PKEY1 = ? AND PKEY2 > ? ) ) AND ROWNUM <= 10000";
    Assert.assertEquals(expected, actual);

    actual =
        new MySqlChunkedQueryManager().generateChunkedQuery(nestedQuery, keys, chunkSize, bucketSize, chunkIndex);
    expected =
        "SELECT * FROM ( SELECT * FROM table ) nestedTab WHERE MOD ( MD5 ( PKEY1 || PKEY2 ) , 10 ) = 3 AND "
            + "( ( PKEY1 > ? ) OR ( PKEY1 = ? AND PKEY2 > ? ) ) LIMIT 10000";
    Assert.assertEquals(expected, actual);
  }
}
