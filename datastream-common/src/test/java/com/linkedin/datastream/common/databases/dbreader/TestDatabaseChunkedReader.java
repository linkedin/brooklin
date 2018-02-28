package com.linkedin.datastream.common.databases.dbreader;

import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import javax.sql.DataSource;

import org.apache.avro.Schema;
import org.mockito.Mockito;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import junit.framework.Assert;

import com.linkedin.datastream.avrogenerator.DatabaseSource;
import com.linkedin.datastream.avrogenerator.SchemaGenerationException;
import com.linkedin.datastream.common.databases.DatabaseColumnRecord;
import com.linkedin.datastream.common.databases.DatabaseRow;
import com.linkedin.datastream.common.databases.MockJDBCConnection;
import com.linkedin.datastream.metrics.DynamicMetricsManager;

import static com.linkedin.datastream.common.databases.dbreader.DatabaseChunkedReaderConfig.*;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.anyInt;


public class TestDatabaseChunkedReader {
  // Dummy table name with 3 columns forming a composite key.
  private static final String TEST_COMPOSITE_KEY_TABLE = "TEST_DB_TEST_TABLE";
  private static final ArrayList<String> TEST_COMPOSITE_PKEYS = new ArrayList<>(Arrays.asList("key1", "key2", "key3"));
  private static final String TEST_SOURCE_QUERY = "SELECT * FROM " + TEST_COMPOSITE_KEY_TABLE + " ORDER BY KEY1, KEY2, KEY3";
  DynamicMetricsManager _dynamicMetricsManager;
  // Dummy table with columns key1, key1, key3 all numbers and timestamp column.
  private static final String TEST_COMPOSITE_KEY_TABLE_SCHEMA_STR =
      "{\"type\":\"record\",\"name\":\"TEST_DB_TEST_TABLE\",\"namespace\":\"com.linkedin.events.testdb\", \"fields\":["
      + "{\"name\":\"key1\",\"type\":[\"null\",\"long\"],\"default\":null,\"meta\":\"dbFieldName=KEY1;dbFieldPosition=0;dbFieldType=NUMBER;\"},"
      + "{\"name\":\"key2\",\"type\":[\"null\",\"long\"],\"default\":null,\"meta\":\"dbFieldName=KEY2;dbFieldPosition=1;dbFieldType=NUMBER;\"},"
      + "{\"name\":\"key3\",\"type\":[\"null\",\"long\"],\"default\":null,\"meta\":\"dbFieldName=KEY3;dbFieldPosition=2;dbFieldType=NUMBER;\"},"
      + "{\"name\":\"timestamp\",\"type\":[\"null\",\"long\"],\"default\":null,\"meta\":\"dbFieldName=TIMESTAMP;dbFieldPosition=3;dbFieldType=TIMESTAMP;\"}"
      + "],\"meta\":\"dbTableName=TEST_DB_TEST_TABLE;pk=key1,key2,key3;\"}";
  private static final Schema TEST_COMPOSITE_KEY_TABLE_SCHEMA = Schema.parse(TEST_COMPOSITE_KEY_TABLE_SCHEMA_STR);

  private static final String TEST_SIMPLE_KEY_TABLE = "TEST_SIMPLE_SCHEMA";
  private static final String TEST_SIMPLE_QUERY = "SELECT * FROM " + TEST_SIMPLE_KEY_TABLE + " ORDER BY KEY1";
  private static final List<String> TEST_SIMPLE_KEYS = Collections.singletonList("key1");
  private static final String TEST_SIMPLE_SCHEMA_STR =
      "{\"type\":\"record\",\"name\":\"SIMPLE_SCHEMA\",\"namespace\":\"com.linkedin.events.simpleschema\", \"fields\":["
          + "{\"name\":\"key1\",\"type\":[\"null\",\"long\"],\"default\":null,\"meta\":\"dbFieldName=KEY1;dbFieldPosition=0;dbFieldType=NUMBER;\"}"
          + "],\"meta\":\"dbTableName=SIMPLE_SCHEMA;pk=key1;\"}";
  private static final Schema TEST_SIMPLE_SCHEMA = Schema.parse(TEST_SIMPLE_SCHEMA_STR);


  @BeforeMethod
  public void setup(Method method) {
    _dynamicMetricsManager = DynamicMetricsManager.createInstance(new MetricRegistry());
  }

  private Properties createTestDBReaderProperties(Integer chunkSize, Integer numBuckets, Integer index) {
    return createTestDBReaderProperties(chunkSize, numBuckets, index, false);
  }

  private Properties createTestDBReaderProperties(Integer chunkSize, Integer numBuckets, Integer index,
      Boolean skipBadMsg) {
    Properties props = new Properties();
    props.setProperty(DB_READER_DOMAIN_CONFIG + "." + QUERY_TIMEOUT_SECS, "10000"); //10 secs
    props.setProperty(DB_READER_DOMAIN_CONFIG + "." + FETCH_SIZE, "100");
    props.setProperty(DB_READER_DOMAIN_CONFIG + "." + SKIP_BAD_MESSAGE, skipBadMsg.toString());
    props.setProperty(DB_READER_DOMAIN_CONFIG + "." + ROW_COUNT_LIMIT, chunkSize.toString());
    props.setProperty(DB_READER_DOMAIN_CONFIG + "." + NUM_CHUNK_BUCKETS, numBuckets.toString());
    props.setProperty(DB_READER_DOMAIN_CONFIG + "." + CHUNK_INDEX, index.toString());
    props.setProperty(DB_READER_DOMAIN_CONFIG + "." + DATABASE_INTERPRETER_CLASS_NAME,
        "com.linkedin.datastream.common.databases.PassThroughSqlTypeInterpreter");
    props.setProperty(DB_READER_DOMAIN_CONFIG + "." + DATABASE_QUERY_MANAGER_CLASS_NAME,
        "com.linkedin.datastream.common.databases.dbreader.OracleChunkedQueryManager");

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
  @Test
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
    Mockito.when(mockDBSource.getPrimaryKeyFields(TEST_COMPOSITE_KEY_TABLE)).thenReturn(TEST_COMPOSITE_PKEYS);
    Mockito.when(mockDBSource.getTableSchema(TEST_COMPOSITE_KEY_TABLE)).thenReturn(TEST_COMPOSITE_KEY_TABLE_SCHEMA);

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
      try (DatabaseChunkedReader reader =
          new DatabaseChunkedReader(props.get(i), mockSources.get(i), "TEST_DB", TEST_SOURCE_QUERY,
              TEST_COMPOSITE_KEY_TABLE, mockDBSource, "testRowCount_" + i)) {
        reader.start();
        for (DatabaseRow row = reader.poll(); row != null; row = reader.poll()) {
          data.get(i).add(row);
        }
      }

      // Coalesce the per call row data into a single list to compared against received data
      List<DatabaseRow> expected = new ArrayList<>();
      dataMapList.get(i).values().forEach(sublist -> expected.addAll(sublist));
      verifyData(data.get(i), expected);
    }
  }


  @Test
  void testSkipBadMessages() throws SQLException, SchemaGenerationException {
    int numBuckets = 4;
    int chunkSize = 3;
    String readerId = "testSkipBadMessages";

    // Create other mocks and parameter list to create the readers.
    DatabaseSource mockDBSource = Mockito.mock(DatabaseSource.class);
    Mockito.when(mockDBSource.getPrimaryKeyFields(anyString())).thenReturn(TEST_SIMPLE_KEYS);
    Mockito.when(mockDBSource.getTableSchema(anyString())).thenReturn(TEST_SIMPLE_SCHEMA);

    Properties props = createTestDBReaderProperties(chunkSize, numBuckets, 0, true);
    DataSource mockDs = Mockito.mock(DataSource.class);
    Connection mockConnection = Mockito.mock(Connection.class);
    PreparedStatement mockStmt = Mockito.mock(PreparedStatement.class);
    ResultSet mockRs = Mockito.mock(ResultSet.class);
    DatabaseColumnRecord field = new DatabaseColumnRecord("key1", 1, Types.INTEGER);

    Mockito.when(mockRs.getObject(1)).thenReturn(field.getValue());
    // First call to ResultSetMetadata.getColumnCount throws exception, the next returns a data,
    // the next throws an exception and the next returns data. We are done with testing, so 4 next() calls return
    // true and the last a false to break poll loop.
    Mockito.when(mockRs.next()).thenReturn(true).thenReturn(true).thenReturn(true).thenReturn(true).thenReturn(false);
    ResultSetMetaData mockRsmd = Mockito.mock(ResultSetMetaData.class);
    Mockito.when(mockRs.getMetaData()).thenReturn(mockRsmd);
    Mockito.when(mockRsmd.getColumnCount()).thenThrow(new SQLException("Bad row - test skip bad message test"))
      .thenReturn(1).thenThrow(new SQLException("Bad row - test skip bad message test")).thenReturn(1);
    Mockito.when(mockRsmd.getColumnName(anyInt())).thenReturn(field.getColName());
    Mockito.when(mockRsmd.getColumnType(anyInt())).thenReturn(field.getSqlType());
    Mockito.when(mockStmt.executeQuery()).thenReturn(mockRs);
    Mockito.when(mockConnection.prepareStatement(anyString())).thenReturn(mockStmt);
    Mockito.when(mockDs.getConnection()).thenReturn(mockConnection);

    int count = 0;
    DatabaseChunkedReader reader =
        new DatabaseChunkedReader(props, mockDs, TEST_SIMPLE_QUERY, "TEST_DB", TEST_SIMPLE_KEY_TABLE, mockDBSource, readerId);
    reader.start();
    for (DatabaseRow row = reader.poll(); row != null; row = reader.poll()) {
      Assert.assertEquals(row, new DatabaseRow(Collections.singletonList(field)));
      count++;
    }
    Assert.assertEquals(2, count);

    String source = String.join(".", "TEST_DB", TEST_SIMPLE_KEY_TABLE);
    String fullMetricName = MetricRegistry.name(DatabaseChunkedReader.class.getSimpleName(), source,
        DatabaseChunkedReaderMetrics.SKIPPED_BAD_MESSAGES_RATE);
    Assert.assertEquals(((Meter) _dynamicMetricsManager.getMetric(fullMetricName)).getCount(), 2);

    fullMetricName = MetricRegistry.name(DatabaseChunkedReader.class.getSimpleName(),
        readerId, DatabaseChunkedReaderMetrics.SKIPPED_BAD_MESSAGES_RATE);
    Assert.assertEquals(((Meter) _dynamicMetricsManager.getMetric(fullMetricName)).getCount(), 2);
    reader.close();
  }
}
