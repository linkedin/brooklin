package com.linkedin.datastream.tools;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import javax.sql.DataSource;

import org.apache.avro.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.datastream.avrogenerator.DatabaseSource;
import com.linkedin.datastream.avrogenerator.SchemaGenerationException;
import com.linkedin.datastream.common.DatabaseRow;
import com.linkedin.datastream.common.DynamicDataSourceFactoryImpl;
import com.linkedin.datastream.dbreader.DatabaseChunkedReader;

import static com.linkedin.datastream.dbreader.DBReaderConfig.*;


public class DatabaseChunkedReaderClient {
  private static final Logger LOG = LoggerFactory.getLogger(DatabaseChunkedReader.class);

  private static Properties createTestDBReaderProperties(Integer numBuckets, Integer index) {
    Properties props = new Properties();
    props.setProperty(DBREADER_DOMAIN_CONFIG + "." + QUERY_TIMEOUT_SECS, "10000"); //10 secs
    props.setProperty(DBREADER_DOMAIN_CONFIG + "." + FETCH_SIZE, "100");
    props.setProperty(DBREADER_DOMAIN_CONFIG + "." + CHUNK_SIZE, "1000");
    props.setProperty(DBREADER_DOMAIN_CONFIG + "." + NUM_CHUNK_BUCKETS, numBuckets.toString());
    props.setProperty(DBREADER_DOMAIN_CONFIG + "." + CHUNK_INDEX, index.toString());
    props.setProperty(DBREADER_DOMAIN_CONFIG + "." + HASH_FUNCTION, "ORA_HASH");
    props.setProperty(DBREADER_DOMAIN_CONFIG + "." + CONCAT_FUNCTION, "CONCAT");
    return props;
  }

  private static DatabaseSource createTestDatabaseSource() {
    return new DatabaseSource() {

      @Override
      public boolean isTable(String schemaName, String tableName) {
        return true;
      }

      @Override
      public boolean isCollection(String schemaName, String fieldTypeName) throws SQLException {
        return false;
      }

      @Override
      public boolean isStruct(String schemaName, String fieldTypeName) throws SQLException {
        return false;
      }

      @Override
      public boolean isPrimitive(String fieldTypeName) throws SQLException {
        return true;
      }

      @Override
      public List<TableMetadata> getTableMetadata(String schemaName, String tableName) throws SQLException {
        return null;
      }

      @Override
      public List<StructMetadata> getStructMetadata(String schemaName, String fieldTypeName) throws SQLException {
        return null;
      }

      @Override
      public CollectionMetadata getCollectionMetadata(String schemaName, String fieldTypeName) throws SQLException {
        return null;
      }

      @Override
      public List<String> getPrimaryKeyFields(String tableName) throws SQLException, SchemaGenerationException {
        ArrayList<String> pkeys = new ArrayList<>();
        if (tableName.equals("TEST_CLOB_TENART")) {
          pkeys.add("ARTICLE_ID");
          pkeys.add("PUBLISH_DATE");
        } else if (tableName.equals("CONNECTIONS")) {
          pkeys.add("SOURCE_ID");
          pkeys.add("DEST_ID");
        }
        return pkeys;
      }

      @Override
      public List<String> getAllFields(String tableName, String dbName) throws SQLException {
        return null;
      }

      @Override
      public Schema getTableSchema(String tableName) {
        Schema schema = Schema.parse("{\"type\":\"record\",\"name\":\"CONNECTIONS\",\"namespace\":\"com.linkedin.events.conns\",\"fields\":[{\"name\":\"sourceId\",\"type\":[\"null\",\"string\"], \"default\":null,\"meta\":\"dbFieldName=SOURCE_ID;dbFieldPosition=0;dbFieldType=NUMBER;numberScale=-127;numberPrecision=0;\"},{\"name\":\"destId\",\"type\":  [\"null\",\"string\"],\"default\":null,\"meta\":\"dbFieldName=DEST_ID;dbFieldPosition=1;dbFieldType=NUMBER;numberScale=-127;numberPrecision=0;\"},{\"name\":  \"active\",\"type\":[\"null\",\"string\"],\"default\":null,\"meta\":\"dbFieldName=ACTIVE;dbFieldPosition=2;dbFieldType=CHAR;\"},{\"name\":\"createDate\",     \"type\":[\"null\",\"long\"],\"default\":null,\"meta\":\"dbFieldName=CREATE_DATE;dbFieldPosition=3;dbFieldType=DATE;\"},{\"name\":\"modifiedDate\",\"type\":  [\"null\",\"long\"],\"default\":null,\"meta\":\"dbFieldName=MODIFIED_DATE;dbFieldPosition=4;dbFieldType=TIMESTAMP;\"},{\"name\":\"txn\",\"type\":[\"null\",   \"string\"],\"default\":null,\"meta\":\"dbFieldName=TXN;dbFieldPosition=5;dbFieldType=NUMBER;numberScale=-127;numberPrecision=0;\"},{\"name\":\"ggModiTs\",   \"type\":[\"null\",\"long\"],\"default\":null,\"meta\":\"dbFieldName=GG_MODI_TS;dbFieldPosition=6;dbFieldType=TIMESTAMP;\"},{\"name\":\"ggStatus\",\"type\":  [\"null\",\"string\"],\"default\":null,\"meta\":\"dbFieldName=GG_STATUS;dbFieldPosition=7;dbFieldType=VARCHAR2;\"},{\"name\":\"deletedTs\",\"type\":          [\"null\",\"long\"],\"default\":null,\"meta\":\"dbFieldName=DELETED_TS;dbFieldPosition=8;dbFieldType=TIMESTAMP;\"}],\"meta\":\"dbTableName=CONNECTIONS;       pk=sourceId,destId;\"}");
        return schema;
      }
    };
  }

  private static DataSource createTestDataSource(String conUri) throws Exception {

    DynamicDataSourceFactoryImpl dataSourceFactory = new DynamicDataSourceFactoryImpl();
    return dataSourceFactory.createOracleDataSource(conUri, -1);
  }


  public static void main(String[] args) throws Exception {
    String conUri = "jdbc:oracle:thin:conns/conns8uat@//ltx1-db01.stg.linkedin.com:1521/EIDB";
    String table = "CONNECTIONS";
    String sourceQuery = "SELECT * FROM " + table + " ORDER BY SOURCE_ID, DEST_ID";
    DatabaseSource databaseSource = createTestDatabaseSource();

    int numBuckets = 10;
    List<DataSource> source = new ArrayList<>();
    List<String> id = new ArrayList<>();
    List<Properties> props = new ArrayList<>();
    Map<Integer, List<DatabaseRow>> data = new HashMap();
    for (int i = 0; i < numBuckets; i++) {
      source.add(createTestDataSource(conUri));
      id.add("TEST_SEED_" + i);
      props.add(createTestDBReaderProperties(numBuckets, i));
      data.put(i, new ArrayList<>());
    }

    // simple test count rows.
    long numRows = 0;

    for (int i = 0; i < numBuckets; i++) {
      DatabaseChunkedReader reader =
          new DatabaseChunkedReader(props.get(i), source.get(i), sourceQuery, table, databaseSource, id.get(i));
      reader.start();
      int count = 0;
      while (true) {
        DatabaseRow row = reader.poll();
        if (row == null) {
          break;
        }
        data.get(i).add(row);
        //process row data
        System.out.println(System.currentTimeMillis() + "\t\t" + row.getRecords().get(0) + "," +
            row.getRecords().get(1) + ", count : " + count);
        count++;
      }

      System.out.println(System.currentTimeMillis() + "\t\tDone one reader: ");
      System.out.println(System.currentTimeMillis() + "\t\tnum rows read " + count);
      numRows += count;
      reader.close();
    }
    System.out.println(System.currentTimeMillis() + "\t\t Total rows read " + numRows);
  }

}
