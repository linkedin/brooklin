package com.linkedin.datastream.connectors.oracle.triggerbased;

import java.util.List;

import org.apache.avro.Schema;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.codahale.metrics.MetricRegistry;
import com.linkedin.datastream.common.Datastream;
import com.linkedin.datastream.common.BrooklinEnvelope;
import com.linkedin.datastream.common.DatastreamException;
import com.linkedin.datastream.metrics.DynamicMetricsManager;
import com.linkedin.datastream.server.DatastreamTaskImpl;
import com.linkedin.datastream.server.ListBackedTransportProvider;
import com.linkedin.datastream.server.api.transport.DatastreamRecordMetadata;
import com.linkedin.datastream.connectors.oracle.triggerbased.consumer.OracleChangeEvent;


public class TestMessageHandler {
  private OracleSource _genericSource;
  private OracleSource _complexSource;

  private static final byte MAGIC_BYTE = 0x0;

  @BeforeClass
  public void setUp() throws Exception {
    // setup the DynamicMetricsManager (required for the test event producer)
    DynamicMetricsManager.createInstance(new MetricRegistry(), "TestMessageHandler");

    // create a source view to run queries for
    _genericSource = new OracleSource("GENERIC_NAME", "Oracle");
    _complexSource = new OracleSource("COMPLEX_NAME", "Oracle");
  }

  @Test
  public void testMessageHandlerBasic() throws Exception {
    Datastream stream = OracleConnectorTestUtils.createDatastream();
    DatastreamTaskImpl task = OracleConnectorTestUtils.createDatastreamTask(stream);

    // create an instance of the MessageHandler
    MessageHandler handler = new MessageHandler(_genericSource, MockSchema.GENERIC_SCHEMA, task);
    List<OracleChangeEvent> events = OracleConnectorTestUtils.generateGenericEvents(100);

    // process the result
    // iterates through all the rows converting them into individual datastreamEvents
    // and then sending those event into the TransportProvider
    for (OracleChangeEvent event : events) {
      handler.processChangeEvent(event);
    }

    // validate that the correct number of events were passed
    // into the stream
    ListBackedTransportProvider transport = OracleConnectorTestUtils.getListBackedTransport(task);
    Assert.assertEquals(transport.getAllEvents().size(), 100);
  }

  @Test
  public void testEncoding() throws Exception {
    MockSchemaRegistry mockSchemaRegistry = new MockSchemaRegistry();
    String schemaID = mockSchemaRegistry.registerSchema(MockSchema.COMPLEX_SCHEMA.getName(), MockSchema.COMPLEX_SCHEMA);

    // create a datastreamTask
    Datastream stream = OracleConnectorTestUtils.createDatastream("oracle/dbName/tableName", schemaID, "ListBackedTransportProvider");
    DatastreamTaskImpl task = OracleConnectorTestUtils.createDatastreamTask(stream);

    // create an instance of the MessageHandler
    MessageHandler handler = new MessageHandler(_complexSource, MockSchema.COMPLEX_SCHEMA, task);
    List<OracleChangeEvent> events = OracleConnectorTestUtils.generateComplexMockEvents(2);


    BrooklinEnvelope envelope = handler.generateBrooklinEnvelope(events.get(1));

    // validate basic encoding on key
    byte[] keyByteArray = (byte[]) envelope.key().get();
    Assert.assertNotNull(keyByteArray);
    Assert.assertFalse(keyByteArray.length == 0);
    Assert.assertFalse(keyByteArray[0] > MAGIC_BYTE);

    // deserialize the key to validate that encoding worked
    long val = 0L;
    int len = keyByteArray.length;
    for (int index = 0; index < len; ++index) {
      byte b = keyByteArray[index];
      val <<= 8;
      val |= (long) (b & 255);
    }
    Assert.assertEquals((long) Long.valueOf(val), 1L);
  }


  @Test
  public void testMessageHandlerWithComplexSchema() throws Exception {
    // create a datastreamTask
    Datastream stream = OracleConnectorTestUtils.createDatastream();
    DatastreamTaskImpl task = OracleConnectorTestUtils.createDatastreamTask(stream);


    // create an instance of the MessageHandler
    MessageHandler handler = new MessageHandler(_complexSource, MockSchema.COMPLEX_SCHEMA, task);
    List<OracleChangeEvent> events = OracleConnectorTestUtils.generateComplexMockEvents(100);

    // process the result
    // iterates through all the rows converting them into individual datastreamEvents
    // and then sending those event into the TransportProvider
    for (OracleChangeEvent event : events) {
      handler.processChangeEvent(event);
    }

    // validate that the correct number of events were passed
    // into the stream
    ListBackedTransportProvider transport = OracleConnectorTestUtils.getListBackedTransport(task);
    Assert.assertEquals(transport.getAllEvents().size(), 100);
  }

  @Test
  public void testDeterminePrimaryKeyFieldWithPk() {
    Schema schema = MockSchema.GENERIC_SCHEMA;
    Assert.assertEquals(MessageHandler.getPrimaryKeyFieldFromSchema(schema), "getLucky");
  }

  @Test(expectedExceptions = DatastreamException.class)
  public void testInvalidSchemaEncoding() throws Exception {
    // create a datastreamTask
    Datastream stream = OracleConnectorTestUtils.createDatastream();
    DatastreamTaskImpl task = OracleConnectorTestUtils.createDatastreamTask(stream);


    // bad Schema
    Schema schema =
        Schema.parse("{\"name\":\"Feedback\",\"meta\":\"\",\"type\":\"record\",\"fields\":[{\"name\":\"key\",\"type\":\"long\"}]}");

    // create an instance of the MessageHandler
    MessageHandler handler = new MessageHandler(_complexSource, schema, task);
    List<OracleChangeEvent> events = OracleConnectorTestUtils.generateComplexMockEvents(1);

    // process the result
    // iterates through all the rows converting them into individual datastreamEvents
    // and then sending those event into the TransportProvider
    for (OracleChangeEvent event : events) {
      handler.processChangeEvent(event);
    }
  }

  @Test
  public void testDeterminePrimaryKeyFieldWithFieldKey() {
    Schema schema = MockSchema.COMPLEX_SCHEMA;
    Assert.assertEquals(MessageHandler.getPrimaryKeyFieldFromSchema(schema), "key");
  }

  @Test
  public void testMessageHandlerErrorState() {
    // create a datastreamTask
    Datastream stream = OracleConnectorTestUtils.createDatastream();
    DatastreamTaskImpl task = OracleConnectorTestUtils.createDatastreamTask(stream);

    MessageHandler handler = new MessageHandler(_genericSource, MockSchema.GENERIC_SCHEMA, task);
    Assert.assertEquals(!handler.isErrorState(), true);

    // trigger onCompletion with error
    DatastreamRecordMetadata md = new DatastreamRecordMetadata("cp", "tp", 1);
    Exception exception = new Exception("testException");
    handler.onCompletion(md, exception);

    Assert.assertEquals(!handler.isErrorState(), false);

    // reset error
    handler.resetErrorState();
    Assert.assertEquals(!handler.isErrorState(), true);
  }
}
