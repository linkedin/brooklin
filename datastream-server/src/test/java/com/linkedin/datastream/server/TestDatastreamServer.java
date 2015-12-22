package com.linkedin.datastream.server;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.linkedin.datastream.DatastreamRestClient;
import com.linkedin.datastream.KafkaTestUtils;
import com.linkedin.datastream.common.AvroUtils;
import com.linkedin.datastream.common.Datastream;
import com.linkedin.datastream.common.DatastreamDestination;
import com.linkedin.datastream.common.DatastreamEvent;
import com.linkedin.datastream.common.DatastreamException;
import com.linkedin.datastream.common.PollUtils;
import com.linkedin.datastream.connectors.DummyBootstrapConnector;
import com.linkedin.datastream.connectors.DummyBootstrapConnectorFactory;
import com.linkedin.datastream.connectors.DummyConnector;
import com.linkedin.datastream.connectors.DummyConnectorFactory;
import com.linkedin.datastream.connectors.file.FileConnector;
import com.linkedin.datastream.connectors.file.FileConnectorFactory;
import com.linkedin.datastream.kafka.KafkaDestination;
import com.linkedin.datastream.server.assignment.BroadcastStrategy;
import com.linkedin.datastream.testutil.DatastreamTestUtils;
import com.linkedin.datastream.testutil.EmbeddedDatastreamCluster;


@Test(singleThreaded = true)
public class TestDatastreamServer {

  private static final Logger LOG = LoggerFactory.getLogger(TestDatastreamServer.class.getName());

  private static final String BROADCAST_STRATEGY = BroadcastStrategy.class.getTypeName();
  private static final String DUMMY_CONNECTOR = DummyConnector.CONNECTOR_TYPE;
  private static final String DUMMY_BOOTSTRAP_CONNECTOR = DummyBootstrapConnector.CONNECTOR_TYPE;
  private static final String TEST_CONNECTOR = FileConnector.CONNECTOR_TYPE;

  public static EmbeddedDatastreamCluster initializeTestDatastreamServerWithBootstrap()
      throws Exception {
    DatastreamServer.INSTANCE.shutdown();
    Map<String, Properties> connectorProperties = new HashMap<>();
    connectorProperties.put(DUMMY_CONNECTOR, getDummyConnectorProperties(true));
    connectorProperties.put(DUMMY_BOOTSTRAP_CONNECTOR, getBootstrapConnectorProperties());
    return EmbeddedDatastreamCluster.newTestDatastreamKafkaCluster(connectorProperties, new Properties(), -1);
  }

  private static Properties getBootstrapConnectorProperties() {
    Properties props = new Properties();
    props.put(DatastreamServer.CONFIG_CONNECTOR_ASSIGNMENT_STRATEGY, BROADCAST_STRATEGY);
    props.put(DatastreamServer.CONFIG_CONNECTOR_FACTORY_CLASS_NAME, DummyBootstrapConnectorFactory.class.getTypeName());
    return props;
  }

  public static EmbeddedDatastreamCluster initializeTestDatastreamServer(Properties override)
      throws Exception {
    DatastreamServer.INSTANCE.shutdown();
    Map<String, Properties> connectorProperties = new HashMap<>();
    connectorProperties.put(DUMMY_CONNECTOR, getDummyConnectorProperties(false));
    EmbeddedDatastreamCluster datastreamKafkaCluster =
        EmbeddedDatastreamCluster.newTestDatastreamKafkaCluster(connectorProperties, override, -1);
    return datastreamKafkaCluster;
  }

  private static Properties getDummyConnectorProperties(boolean boostrap) {
    Properties props = new Properties();
    props.put(DatastreamServer.CONFIG_CONNECTOR_ASSIGNMENT_STRATEGY, BROADCAST_STRATEGY);
    props.put(DatastreamServer.CONFIG_CONNECTOR_FACTORY_CLASS_NAME, DummyConnectorFactory.class.getTypeName());
    if (boostrap) {
      props.put(DatastreamServer.CONFIG_CONNECTOR_BOOTSTRAP_TYPE, DUMMY_BOOTSTRAP_CONNECTOR);
    }
    props.put("dummyProperty", "dummyValue");
    return props;
  }

  @Test
  public void testDatastreamServerBasics()
      throws Exception {
    initializeTestDatastreamServer(null);
    initializeTestDatastreamServerWithBootstrap();
  }

  @Test
  public void testCreateDatastreamOfTestConnector_ProduceEvents_ReceiveEvents()
      throws Exception {
    EmbeddedDatastreamCluster datastreamCluster = initializeTestDatastreamServerWithFileConnector();
    int totalEvents = 10;
    datastreamCluster.startup();
    File testFile = new File("/tmp/testFile1");
    testFile.createNewFile();
    Datastream fileDatastream1 = DatastreamTestUtils.createDatastream(FileConnector.CONNECTOR_TYPE,"file_" + testFile.getName(),
        testFile.getAbsolutePath());
    String restUrl = String.format("http://localhost:%d/", datastreamCluster.getDatastreamPort());
    DatastreamRestClient restClient = new DatastreamRestClient(restUrl);
    restClient.createDatastream(fileDatastream1);
    Collection<String> events = generateStrings(totalEvents);
    FileUtils.writeLines(testFile, events);
    DatastreamDestination destination = getDatastreamDestination(restClient, fileDatastream1);
    KafkaDestination kafkaDestination = KafkaDestination.parseKafkaDestinationUri(destination.getConnectionString());
    final int[] numberOfMessages = {0};
    List<String> eventsReceived = new ArrayList<>();
    KafkaTestUtils.readTopic(kafkaDestination.topicName(), 0, datastreamCluster.getBrokerList(), (key, value) -> {
          DatastreamEvent datastreamEvent = AvroUtils.decodeAvroSpecificRecord(DatastreamEvent.class, value);
          String eventValue = new String(datastreamEvent.payload.array());
          eventsReceived.add(eventValue);
          numberOfMessages[0]++;
          return numberOfMessages[0] < totalEvents;
        });

    LOG.info("Events Received " + eventsReceived);
    LOG.info("Events Written to file " + events);

    Assert.assertTrue(eventsReceived.containsAll(events));

    datastreamCluster.shutdown();
  }

  private DatastreamDestination getDatastreamDestination(DatastreamRestClient restClient, Datastream fileDatastream1) {
    Boolean pollResult = PollUtils.poll(() -> {
      Datastream ds = null;
      try {
        ds = restClient.getDatastream(fileDatastream1.getName());
      } catch (DatastreamException e) {
        throw new RuntimeException("GetDatastream threw an exception", e);
      }
      return ds.getDestination() != null;
    }, 500, 60000);

    if (pollResult) {
      try {
        return restClient.getDatastream(fileDatastream1.getName()).getDestination();
      } catch (DatastreamException e) {
        throw new RuntimeException("GetDatastream threw an exception", e);
      }
    } else {
      throw new RuntimeException("Destination was not populated before the timeout");
    }
  }

  private Collection<String> generateStrings(int numberOfEvents) {
    Collection<String> generatedValues = new ArrayList<>();
    for (int index = 0; index < numberOfEvents; index++) {
      generatedValues.add("Value_" + index);
    }
    return generatedValues;
  }

  private EmbeddedDatastreamCluster initializeTestDatastreamServerWithFileConnector()
      throws IOException, DatastreamException {
    DatastreamServer.INSTANCE.shutdown();
    Map<String, Properties> connectorProperties = new HashMap<>();
    connectorProperties.put(TEST_CONNECTOR, getTestConnectorProperties());
    EmbeddedDatastreamCluster datastreamKafkaCluster =
        EmbeddedDatastreamCluster.newTestDatastreamKafkaCluster(connectorProperties, new Properties(), -1);
    return datastreamKafkaCluster;
  }

  private Properties getTestConnectorProperties() {
    Properties props = new Properties();
    props.put(DatastreamServer.CONFIG_CONNECTOR_ASSIGNMENT_STRATEGY, BROADCAST_STRATEGY);
    props.put(DatastreamServer.CONFIG_CONNECTOR_FACTORY_CLASS_NAME, FileConnectorFactory.class.getTypeName());
    return props;
  }
}
