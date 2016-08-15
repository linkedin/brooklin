package com.linkedin.datastream.connectors.mysql;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.linkedin.datastream.DatastreamRestClient;
import com.linkedin.datastream.common.Datastream;
import com.linkedin.datastream.common.DatastreamEvent;
import com.linkedin.datastream.common.DatastreamEventMetadata;
import com.linkedin.datastream.common.DatastreamException;
import com.linkedin.datastream.common.PollUtils;
import com.linkedin.datastream.connectors.mysql.MysqlConnector.SourceType;
import com.linkedin.datastream.connectors.mysql.or.ColumnInfo;
import com.linkedin.datastream.connectors.mysql.or.InMemoryTableInfoProvider;
import com.linkedin.datastream.server.DatastreamProducerRecord;
import com.linkedin.datastream.server.DatastreamServer;
import com.linkedin.datastream.server.EmbeddedDatastreamCluster;
import com.linkedin.datastream.server.InMemoryTransportProvider;
import com.linkedin.datastream.server.InMemoryTransportProviderFactory;
import com.linkedin.datastream.server.assignment.LoadbalancingStrategyFactory;
import com.linkedin.datastream.testutil.DatastreamTestUtils;


public class TestMysqlConnector {

  private static final Logger LOG = LoggerFactory.getLogger(TestMysqlConnector.class.getName());

  private EmbeddedDatastreamCluster _datastreamCluster;

  private static final String DB1_NAME = "testdb1";
  private static final String TABLE1_NAME = "testtable1";
  private static final String TABLE2_NAME = "testtable2";

  private static final String BINLOG_FILENAME = "mysql-bin.000001";

  @BeforeMethod
  public void setup() throws Exception {
    _datastreamCluster = initializeTestDatastreamServerWithMysqlConnector(new Properties());
    _datastreamCluster.startup();
    InMemoryTableInfoProvider.getTableInfoProvider()
        .addTableInfo(DB1_NAME, TABLE1_NAME,
            Arrays.asList(new ColumnInfo("id", true), new ColumnInfo("name"), new ColumnInfo("description")));

    InMemoryTableInfoProvider.getTableInfoProvider()
        .addTableInfo(DB1_NAME, TABLE2_NAME,
            Arrays.asList(new ColumnInfo("id", true), new ColumnInfo("name"), new ColumnInfo("description")));
  }

  @Test
  public void testMysqlDatastreamSingleTable() throws IOException, DatastreamException {
    Datastream datastream =
        createMysqlDatastream("mysqldatastream1", DB1_NAME, TABLE1_NAME, "datastream-mysql-connector/src/test/data");
    Assert.assertTrue(datastream.hasDestination());
    LOG.info("Datastream created " + datastream);
    String connectionString = datastream.getDestination().getConnectionString();
    InMemoryTransportProvider transportProvider = InMemoryTransportProviderFactory.getTransportProvider();
    boolean pollResult = PollUtils.poll(() -> transportProvider.getTotalEventsReceived(connectionString) == 9,
        Duration.ofMillis(100).toMillis(), Duration.ofMinutes(2).toMillis());
    Assert.assertTrue(pollResult);
    Assert.assertEquals(transportProvider.getRecordsReceived().get(connectionString).size(), 3);

    DatastreamProducerRecord record1 = transportProvider.getRecordsReceived().get(connectionString).get(0);
    DatastreamProducerRecord record2 = transportProvider.getRecordsReceived().get(connectionString).get(1);
    DatastreamProducerRecord record3 = transportProvider.getRecordsReceived().get(connectionString).get(2);

    // Validate the record1
    Assert.assertEquals(record1.getPartition().get().intValue(), 0);
    Assert.assertEquals(record1.getEvents().size(), 4);
    MysqlCheckpoint checkpoint1 = new MysqlCheckpoint(record1.getCheckpoint());
    LOG.info("Source checkpoint1 =" + checkpoint1);
    Assert.assertEquals(checkpoint1.getBinlogFileName(), BINLOG_FILENAME);
    Assert.assertEquals(checkpoint1.getTransactionId(), 3);

    List<DatastreamEvent> events = getEventsFromRecord(record1);
    LOG.info("Events from record1 " + events);
    events.stream()
        .forEach(x -> Assert.assertEquals(x.metadata.get(DatastreamEventMetadata.OPCODE),
            DatastreamEventMetadata.OpCode.INSERT.toString()));
    events.stream().forEach(x -> Assert.assertEquals(x.metadata.get(DatastreamEventMetadata.TABLE), TABLE1_NAME));

    // Validate the record2
    Assert.assertEquals(record2.getPartition().get().intValue(), 0);
    Assert.assertEquals(record2.getEvents().size(), 4);
    MysqlCheckpoint checkpoint2 = new MysqlCheckpoint(record2.getCheckpoint());
    LOG.info("Source checkpoint2 =" + checkpoint2);
    Assert.assertEquals(checkpoint2.getBinlogFileName(), BINLOG_FILENAME);
    Assert.assertEquals(checkpoint2.getTransactionId(), 4);

    events = getEventsFromRecord(record2);
    LOG.info("Events from record2 " + events);
    events.stream()
        .forEach(x -> Assert.assertEquals(x.metadata.get(DatastreamEventMetadata.OPCODE),
            DatastreamEventMetadata.OpCode.INSERT.toString()));
    events.stream().forEach(x -> Assert.assertEquals(x.metadata.get(DatastreamEventMetadata.TABLE), TABLE1_NAME));

    // Validate the record3
    Assert.assertEquals(record3.getPartition().get().intValue(), 0);
    Assert.assertEquals(record3.getEvents().size(), 1);
    MysqlCheckpoint checkpoint3 = new MysqlCheckpoint(record3.getCheckpoint());
    LOG.info("Source checkpoint3 = " + checkpoint3);
    Assert.assertEquals(checkpoint3.getBinlogFileName(), BINLOG_FILENAME);
    Assert.assertEquals(checkpoint3.getTransactionId(), 7);

    events = getEventsFromRecord(record3);
    LOG.info("Events from record3 " + events);
    Assert.assertEquals(events.get(0).metadata.get(DatastreamEventMetadata.OPCODE),
        DatastreamEventMetadata.OpCode.UPDATE.toString());
    Assert.assertEquals(events.get(0).metadata.get(DatastreamEventMetadata.TABLE), TABLE1_NAME);
  }

  private List<DatastreamEvent> getEventsFromRecord(DatastreamProducerRecord record) {
    return record.getEvents().stream().map(x -> (DatastreamEvent) x.getValue()).collect(Collectors.toList());
  }

  @Test
  public void testMysqlDatastreamAllTablesInDb() throws IOException, DatastreamException {
    Datastream datastream = createMysqlDatastream("mysqldatastream1", DB1_NAME, MysqlSource.ALL_TABLES,
        "datastream-mysql-connector/src/test/data");
    Assert.assertTrue(datastream.hasDestination());
    LOG.info("Datastream created " + datastream);
    String connectionString = datastream.getDestination().getConnectionString();
    InMemoryTransportProvider transportProvider = InMemoryTransportProviderFactory.getTransportProvider();
    boolean pollResult = PollUtils.poll(() -> transportProvider.getTotalEventsReceived(connectionString) == 16,
        Duration.ofMillis(100).toMillis(), Duration.ofMinutes(2).toMillis());
    Assert.assertTrue(pollResult);
    Assert.assertEquals(transportProvider.getRecordsReceived().get(connectionString).size(), 5);

    DatastreamProducerRecord record1 = transportProvider.getRecordsReceived().get(connectionString).get(0);
    DatastreamProducerRecord record2 = transportProvider.getRecordsReceived().get(connectionString).get(1);
    DatastreamProducerRecord record3 = transportProvider.getRecordsReceived().get(connectionString).get(2);
    DatastreamProducerRecord record4 = transportProvider.getRecordsReceived().get(connectionString).get(3);
    DatastreamProducerRecord record5 = transportProvider.getRecordsReceived().get(connectionString).get(4);

    // Validate the record1
    Assert.assertEquals(record1.getPartition().get().intValue(), 0);
    Assert.assertEquals(record1.getEvents().size(), 4);
    MysqlCheckpoint checkpoint1 = new MysqlCheckpoint(record1.getCheckpoint());
    LOG.info("Source checkpoint1 =" + checkpoint1);
    Assert.assertEquals(checkpoint1.getBinlogFileName(), BINLOG_FILENAME);
    Assert.assertEquals(checkpoint1.getTransactionId(), 3);

    List<DatastreamEvent> events = getEventsFromRecord(record1);
    LOG.info("Events from record1 " + events);
    events.stream()
        .forEach(x -> Assert.assertEquals(x.metadata.get(DatastreamEventMetadata.OPCODE),
            DatastreamEventMetadata.OpCode.INSERT.toString()));
    events.stream().forEach(x -> Assert.assertEquals(x.metadata.get(DatastreamEventMetadata.TABLE), TABLE1_NAME));

    // Validate the record2
    Assert.assertEquals(record2.getPartition().get().intValue(), 0);
    Assert.assertEquals(record2.getEvents().size(), 4);
    MysqlCheckpoint checkpoint2 = new MysqlCheckpoint(record2.getCheckpoint());
    LOG.info("Source checkpoint2 =" + checkpoint2);
    Assert.assertEquals(checkpoint2.getBinlogFileName(), BINLOG_FILENAME);
    Assert.assertEquals(checkpoint2.getTransactionId(), 4);

    events = getEventsFromRecord(record2);
    LOG.info("Events from record2 " + events);
    events.stream()
        .forEach(x -> Assert.assertEquals(x.metadata.get(DatastreamEventMetadata.OPCODE),
            DatastreamEventMetadata.OpCode.INSERT.toString()));
    events.stream().forEach(x -> Assert.assertEquals(x.metadata.get(DatastreamEventMetadata.TABLE), TABLE1_NAME));

    // Validate the record3
    Assert.assertEquals(record3.getPartition().get().intValue(), 0);
    Assert.assertEquals(record3.getEvents().size(), 6);
    MysqlCheckpoint checkpoint3 = new MysqlCheckpoint(record3.getCheckpoint());
    LOG.info("Source checkpoint3 = " + checkpoint3);
    Assert.assertEquals(checkpoint3.getBinlogFileName(), BINLOG_FILENAME);
    Assert.assertEquals(checkpoint3.getTransactionId(), 6);

    events = getEventsFromRecord(record3);
    LOG.info("Events from record3 " + events);
    events.stream()
        .forEach(x -> Assert.assertEquals(x.metadata.get(DatastreamEventMetadata.OPCODE),
            DatastreamEventMetadata.OpCode.INSERT.toString()));
    events.stream().forEach(x -> Assert.assertEquals(x.metadata.get(DatastreamEventMetadata.TABLE), TABLE2_NAME));

    // Validate the record4
    Assert.assertEquals(record4.getPartition().get().intValue(), 0);
    Assert.assertEquals(record4.getEvents().size(), 1);
    MysqlCheckpoint checkpoint4 = new MysqlCheckpoint(record4.getCheckpoint());
    LOG.info("Source checkpoint4 = " + checkpoint4);
    Assert.assertEquals(checkpoint4.getBinlogFileName(), BINLOG_FILENAME);
    Assert.assertEquals(checkpoint4.getTransactionId(), 7);

    events = getEventsFromRecord(record4);
    LOG.info("Events from record4 " + events);
    events.stream()
        .forEach(x -> Assert.assertEquals(x.metadata.get(DatastreamEventMetadata.OPCODE),
            DatastreamEventMetadata.OpCode.UPDATE.toString()));
    events.stream().forEach(x -> Assert.assertEquals(x.metadata.get(DatastreamEventMetadata.TABLE), TABLE1_NAME));

    // Validate the record5
    Assert.assertEquals(record5.getPartition().get().intValue(), 0);
    Assert.assertEquals(record5.getEvents().size(), 1);
    MysqlCheckpoint checkpoint5 = new MysqlCheckpoint(record5.getCheckpoint());
    LOG.info("Source checkpoint5 = " + checkpoint5);
    Assert.assertEquals(checkpoint5.getBinlogFileName(), BINLOG_FILENAME);
    Assert.assertEquals(checkpoint5.getTransactionId(), 8);

    events = getEventsFromRecord(record5);
    LOG.info("Events from record5 " + events);
    events.stream()
        .forEach(x -> Assert.assertEquals(x.metadata.get(DatastreamEventMetadata.OPCODE),
            DatastreamEventMetadata.OpCode.DELETE.toString()));
    events.stream().forEach(x -> Assert.assertEquals(x.metadata.get(DatastreamEventMetadata.TABLE), TABLE2_NAME));
  }

  public static EmbeddedDatastreamCluster initializeTestDatastreamServerWithMysqlConnector(Properties override)
      throws Exception {
    Map<String, Properties> connectorProperties = new HashMap<>();
    connectorProperties.put(MysqlConnector.CONNECTOR_NAME, buildMysqlConnectorProperties());
    return EmbeddedDatastreamCluster.newTestDatastreamCluster(connectorProperties, override);
  }

  private static Properties buildMysqlConnectorProperties() {
    Properties props = new Properties();
    props.put(DatastreamServer.CONFIG_CONNECTOR_ASSIGNMENT_STRATEGY_FACTORY,
        LoadbalancingStrategyFactory.class.getTypeName());
    props.put(DatastreamServer.CONFIG_CONNECTOR_FACTORY_CLASS_NAME, MysqlConnectorFactory.class.getTypeName());
    props.put(MysqlConnector.CFG_MYSQL_SOURCE_TYPE, SourceType.MYSQLBINLOG.toString());
    props.put(MysqlConnector.CFG_MYSQL_SERVER_ID, String.valueOf(101));
    return props;
  }

  private Datastream createMysqlDatastream(String datastreamName, String databaseName, String tableName,
      String binlogFolder) throws IOException, DatastreamException {

    MysqlSource source = new MysqlSource(binlogFolder, databaseName, tableName);
    Datastream mysqlDatastream =
        DatastreamTestUtils.createDatastream(MysqlConnector.CONNECTOR_NAME, datastreamName, source.toString());

    DatastreamRestClient restClient = _datastreamCluster.createDatastreamRestClient();
    restClient.createDatastream(mysqlDatastream);
    return getPopulatedDatastream(restClient, mysqlDatastream);
  }

  private Datastream getPopulatedDatastream(DatastreamRestClient restClient, Datastream datastream) {
    Boolean pollResult = PollUtils.poll(() -> {
      Datastream ds = null;
      ds = restClient.getDatastream(datastream.getName());
      return ds.hasDestination() && ds.getDestination().hasConnectionString() && !ds.getDestination()
          .getConnectionString()
          .isEmpty();
    }, 500, 60000);

    if (pollResult) {
      return restClient.getDatastream(datastream.getName());
    } else {
      throw new RuntimeException("Destination was not populated before the timeout");
    }
  }

  @AfterMethod
  public void cleanup() {
    if (_datastreamCluster != null) {
      _datastreamCluster.shutdown();
    }
  }
}
