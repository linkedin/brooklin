/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.common.databases.dbreader;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import junit.framework.Assert;


/**
 * Base class for {@link ChunkedQueryManager} implementation tests.
 */
public class TestChunkedQueryManagerBase {
  protected static final String NESTED_QUERY = "SELECT * FROM TABLE";
  protected static final List<String> KEY = Collections.singletonList("KEY1");
  protected static final List<String> KEYS = Arrays.asList("KEY1", "KEY2");
  protected static final long CHUNK_SIZE = 10;
  protected static final int PARTITION_COUNT = 10;
  protected static final List<Integer> PARTITION = Collections.singletonList(3);
  protected static final List<Integer> PARTITIONS = Arrays.asList(2, 5);

  protected void testQueryString(ChunkedQueryManager manager, String firstExpected, String chunkedExpected,
      String nestedQuery, List<String> keys, long chunkSize, int partitionCount, List<Integer> partitions) {
    String actual;
    actual = manager.generateFirstQuery(nestedQuery, keys, chunkSize, partitionCount, partitions);
    Assert.assertEquals(firstExpected, actual);

    actual = manager.generateChunkedQuery(nestedQuery, keys, chunkSize, partitionCount, partitions);
    Assert.assertEquals(chunkedExpected, actual);
  }
}
