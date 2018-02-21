package com.linkedin.datastream.dbreader;

import java.lang.reflect.Method;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import javax.sql.DataSource;

import org.apache.avro.Schema;
import org.mockito.Mockito;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import junit.framework.Assert;

import com.linkedin.datastream.avrogenerator.DatabaseSource;
import com.linkedin.datastream.common.DatabaseRow;

import static com.linkedin.datastream.dbreader.DatabaseChunkedReaderConfig.*;


public class TestDBReader {
  // Dummy table name with 3 columns forming a composite key.
  private static final String TEST_TABLE = "TEST_DB_TEST_TABLE";
  private static final ArrayList<String> TEST_PKEYS = new ArrayList<>(Arrays.asList("key1", "key2", "key3"));
  private static final String TEST_SOURCE_QUERY = "SELECT * FROM " + TEST_TABLE + " ORDER BY KEY1, KEY2, KEY3";

  // Dummy table with columns key1, key1, key3 all numbers and timestamp column.
  private static final String TEST_TABLE_SCHEMA_STR =
      "{\"type\":\"record\",\"name\":\"TEST_DB_TEST_TABLE\",\"namespace\":\"com.linkedin.events.testdb\", \"fields\":["
      + "{\"name\":\"key1\",\"type\":[\"null\",\"long\"],\"default\":null,\"meta\":\"dbFieldName=KEY1;dbFieldPosition=0;dbFieldType=NUMBER;\"},"
      + "{\"name\":\"key2\",\"type\":[\"null\",\"long\"],\"default\":null,\"meta\":\"dbFieldName=KEY2;dbFieldPosition=1;dbFieldType=NUMBER;\"},"
      + "{\"name\":\"key3\",\"type\":[\"null\",\"long\"],\"default\":null,\"meta\":\"dbFieldName=KEY3;dbFieldPosition=2;dbFieldType=NUMBER;\"},"
      + "{\"name\":\"timestamp\",\"type\":[\"null\",\"long\"],\"default\":null,\"meta\":\"dbFieldName=TIMESTAMP;dbFieldPosition=3;dbFieldType=TIMESTAMP;\"}"
      + "],\"meta\":\"dbTableName=TEST_DB_TEST_TABLE;pk=key1,key2,key3;\"}";
  private static final Schema TEST_TABLE_SCHEMA = Schema.parse(TEST_TABLE_SCHEMA_STR);

  @BeforeMethod
  public void setup(Method method) {
  }

  /**
   * Test query string when a single primary key is involved in chunking
   */
  @Test
  public void testQueryStringPrimaryKey() {
    String nestedQuery = "SELECT * FROM table";
    String pkeyString = "PKEY";
    LinkedHashMap<String, Object> keyMap = new LinkedHashMap<>();
    keyMap.put(pkeyString, 65789);
    long chunkSize = 10000;
    long bucketSize = 10;
    long chunkIndex = 3;

    String actual =
        DatabaseChunkedReader.getChunkedQuery(nestedQuery, pkeyString, keyMap, chunkSize, bucketSize, chunkIndex,
            "ORA_HASH", "CONCAT");

    String expected =
        "SELECT * FROM ( SELECT * FROM table ) WHERE ORA_HASH ( PKEY , 10 ) = 3 AND ( ( PKEY > ? ) ) AND ROWNUM <= 10000";
    Assert.assertEquals(expected, actual);
  }

  /**
   * Test chunking predicate generation for simple and composite keys
   */
  @Test
  public void testChunkKeyPredicate() {
    LinkedHashMap<String, Object> keyMap = new LinkedHashMap<>();
    String pkey1 = "PKEY1";
    keyMap.put(pkey1, 65789);

    String actual = DatabaseChunkedReader.generateKeyChunkingPredicate(keyMap);
    String expected = "( ( PKEY1 > ? ) )";
    Assert.assertEquals(expected, actual);

    String pkey2 = "PKEY2";
    String pkey3 = "PKEY3";
    keyMap.put(pkey2, 4648490);
    keyMap.put(pkey3, 46484);

    actual = DatabaseChunkedReader.generateKeyChunkingPredicate(keyMap);
    expected = "( ( PKEY1 > ? ) OR ( PKEY1 = ? AND PKEY2 > ? ) OR ( PKEY1 = ? AND PKEY2 = ? AND PKEY3 > ? ) )";
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
    String pkey1 = "PKEY1";
    String pkey2 = "PKEY2";
    String pkeyString = pkey1 + "," + pkey2;
    LinkedHashMap<String, Object> keyMap = new LinkedHashMap<>();
    keyMap.put(pkey1, 65789);
    keyMap.put(pkey2, 4648490);
    long chunkSize = 10000;
    long bucketSize = 10;
    long chunkIndex = 3;

    String actual =
        DatabaseChunkedReader.getChunkedQuery(nestedQuery, pkeyString, keyMap, chunkSize, bucketSize, chunkIndex,
            "ORA_HASH", "CONCAT");
    String expected = "SELECT * FROM ( SELECT * FROM table ) WHERE ORA_HASH ( CONCAT ( PKEY1,PKEY2 ) , 10 ) = 3 AND "
        + "( ( PKEY1 > ? ) OR ( PKEY1 = ? AND PKEY2 > ? ) ) AND ROWNUM <= 10000";
    Assert.assertEquals(expected, actual);
  }

  private Properties createTestDBReaderProperties(Integer chunkSize, Integer numBuckets, Integer index) {
    Properties props = new Properties();
    props.setProperty(DBREADER_DOMAIN_CONFIG + "." + QUERY_TIMEOUT_SECS, "10000"); //10 secs
    props.setProperty(DBREADER_DOMAIN_CONFIG + "." + FETCH_SIZE, "100");
    props.setProperty(DBREADER_DOMAIN_CONFIG + "." + CHUNK_SIZE, chunkSize.toString());
    props.setProperty(DBREADER_DOMAIN_CONFIG + "." + NUM_CHUNK_BUCKETS, numBuckets.toString());
    props.setProperty(DBREADER_DOMAIN_CONFIG + "." + CHUNK_INDEX, index.toString());
    props.setProperty(DBREADER_DOMAIN_CONFIG + "." + HASH_FUNCTION, "ORA_HASH");
    props.setProperty(DBREADER_DOMAIN_CONFIG + "." + CONCAT_FUNCTION, "CONCAT");
    props.setProperty(DBREADER_DOMAIN_CONFIG + "." + DATABASE_INTERPRETER_CLASS_NAME,
        "com.linkedin.datastream.common.PassThroughSqlTypeInterpreter");
    return props;
  }

  private void verifyData(List<DatabaseRow> actual, List<DatabaseRow> expected) {
    Assert.assertEquals(expected.size(), actual.size());
    for (int i = 0; i < actual.size(); i++) {
      Assert.assertEquals(expected.get(i), actual.get(i));
    }
  }

  /**
   * Verify the total rows read and per read row count and data. Test focuses on generation of the query predicate based
   * on previous values. The Mocks are setup to expect specific values.
   * @throws Exception
   */
  @Test(enabled = true)
  public void testRowCount() throws Exception {
    int numBuckets = 4;
    int chunkSize = 3;

    /* Test uses 4 readers with the expects the following results when reading 'TEST_DB_TEST_TABLE'
       Reader0 :
         Call0 : 2 rows returned.
         // No more calls expected.
       Reader1 :
         Call0 : 3 rows returned.
         Call1 : 0 rows returned.
         // No more calls expected.
       Reader2 :
         Call0 : 3 rows returned.
         Call1 : 2 rows returned.
         // No more calls expected.
       Reader3 :
         Call0 : 3 rows returned.
         Call1 : 3 rows returned.
         Call2 : 0 rows returned.
         // No more calls expected.259


       Total rows 16. We will reuse 6 unique rows generated for the table for each reader.
       We will use the DatabaseColumnRecord and DatabaseRow classes to represent the data.
    */

    // Generate row data. For this test, the keys are not very important. So we only increment key3 to generate new keys.
    List<DatabaseRow> rows = new ArrayList<>();
    for (int i = 0; i < 6; i++) {
        rows.add(new DatabaseRow().addField("key1", 0, Types.NUMERIC)
            .addField("key2", 0, Types.NUMERIC)
            .addField("key3", i, Types.NUMERIC)
            .addField("timestamp", System.currentTimeMillis(), Types.TIMESTAMP));
    }

    // Generate the argument list to pass to the mocks. The MockJDBCPreparedStatement is set up to expect
    // specific set of calls to setObject method with specific arguments. If it matches, it will return a
    // specific set of rows for the next executeQuery call and expect subsequent setObject calls to be made
    // with values based on returned result.

    // Map of Reader thread number -> Iteration of setObject call -> ParameterIndex passed in call to setObject -> Value passed in call to setObject
    Map<Integer, Map<Integer, Map<Integer, Object>>> keyMapList = new HashMap<>();

    // Since each reader is reading from the same unique 6 rows, we just generate the param index to
    // value map once for each successive call to setObject sequence and use that for all readers.
    // Since there are 3 keys, there will be six calls to setObject for the below query.
    // ( ( PKEY1 > ? ) OR ( PKEY1 = ?  AND PKEY2 > ? ) OR ( PKEY1 = ?  AND PKEY2 = ?  AND PKEY3 > ? ) )
    Map<Integer, Object> firstCallkeyMap = new HashMap<>();
    Map<Integer, Object> secondCallKeyMap = new HashMap<>();

    int lastRowIndex = chunkSize - 1;
    int[] rowFieldIndexToFetch = {0, 0, 1, 0, 1, 2};
    for (int i = 0; i < 6; i++) {
      firstCallkeyMap.put(i + 1, rows.get(lastRowIndex).getRecords().get(rowFieldIndexToFetch[i]));
    }
    lastRowIndex = (chunkSize * 2) - 1;
    for (int i = 0; i < 6; i++) {
      secondCallKeyMap.put(i + 1, rows.get(lastRowIndex).getRecords().get(rowFieldIndexToFetch[i]));
    }

    // Reader 0 should not have any calls to setObject. So no key for 0

    // Reader 1 should have one set of calls to setObject.
    Map<Integer, Map<Integer, Object>> reader1CallMap = new HashMap<>();
    reader1CallMap.put(0, firstCallkeyMap);
    keyMapList.put(1, reader1CallMap);

    // Reader 2 should have one set of calls to setObject and exactly same values as Reader1's call
    Map<Integer, Map<Integer, Object>> reader2CallMap = new HashMap<>();
    reader2CallMap.put(0, firstCallkeyMap);
    keyMapList.put(2, reader2CallMap);

    // Reader 3 should have 2 calls to setObject. First one same as Readers1's call.
    Map<Integer, Map<Integer, Object>> reader3CallMap = new HashMap<>();
    reader3CallMap.put(0, firstCallkeyMap);
    reader3CallMap.put(1, secondCallKeyMap);
    keyMapList.put(3, reader3CallMap);

    //Reader thread index -> Iteration of executeQuery call -> ResultSet row list to return for it
    Map<Integer, Map<Integer, List<DatabaseRow>>> dataMapList = new HashMap<>();

    // Reader 0 reads 2 rows for the first call
    Map<Integer, List<DatabaseRow>> reader0Data = new HashMap<>();
    reader0Data.put(0, rows.subList(0, 2));
    dataMapList.put(0, reader0Data);

    // Reader 1 reads 3 rows for the first call and 0 for the second
    Map<Integer, List<DatabaseRow>> reader1Data = new HashMap<>();
    reader1Data.put(0, rows.subList(0, 3));
    reader1Data.put(1, Collections.emptyList());
    dataMapList.put(1, reader1Data);

    // Reader 2 reads 3 rows for the first call and 2 for the second
    Map<Integer, List<DatabaseRow>> reader2Data = new HashMap<>();
    reader2Data.put(0, rows.subList(0, 3));
    reader2Data.put(1, rows.subList(3, 5));
    dataMapList.put(2, reader2Data);

    // Reader 3 reads 3 rows for the first call and 3 for the second and 0 for the 3rd
    Map<Integer, List<DatabaseRow>> reader3Data = new HashMap<>();
    reader3Data.put(0, rows.subList(0, 3));
    reader3Data.put(1, rows.subList(3, 6));
    reader3Data.put(2, Collections.emptyList());
    dataMapList.put(3, reader3Data);


    // Create other mocks and parameter list to create the readers.
    DatabaseSource mockDBSource = Mockito.mock(DatabaseSource.class);
    Mockito.when(mockDBSource.getPrimaryKeyFields(TEST_TABLE)).thenReturn(TEST_PKEYS);
    Mockito.when(mockDBSource.getTableSchema(TEST_TABLE)).thenReturn(TEST_TABLE_SCHEMA);

    List<Properties> props = new ArrayList<>();
    List<DataSource> mockSources = new ArrayList<>();

    Map<Integer, List<DatabaseRow>> data = new HashMap<>();
    for (int i = 0; i < numBuckets; i++) {
      props.add(createTestDBReaderProperties(chunkSize, numBuckets, i));
      data.put(i, new ArrayList<>());
      DataSource mockDs = Mockito.mock(DataSource.class);
          Mockito.when(mockDs.getConnection()).thenReturn(new MockJDBCConnection(keyMapList.get(i), dataMapList.get(i)));
      mockSources.add(mockDs);
    }

    for (int i = 0; i < numBuckets; i++) {
      DatabaseChunkedReader reader =
          new DatabaseChunkedReader(props.get(i), mockSources.get(i), TEST_SOURCE_QUERY, TEST_TABLE, mockDBSource, "testRowCount_" + i);
      reader.start();

      while (true) {
        DatabaseRow row = reader.poll();
        if (row == null) {
          break;
        }
        data.get(i).add(row);
      }

      reader.close();

      // Coalesce the per call row data into a single list to compared against received data
      List<DatabaseRow> expected = new ArrayList<>();
      dataMapList.get(i).values().forEach(sublist -> expected.addAll(sublist));
      verifyData(data.get(i), expected);
    }
  }
}
