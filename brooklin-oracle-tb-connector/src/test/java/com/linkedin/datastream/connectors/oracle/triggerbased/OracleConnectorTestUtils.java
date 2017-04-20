package com.linkedin.datastream.connectors.oracle.triggerbased;

import java.sql.Types;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

import com.linkedin.data.template.StringMap;
import com.linkedin.datastream.common.DatastreamDestination;
import com.linkedin.datastream.common.DatastreamMetadataConstants;
import com.linkedin.datastream.common.DatastreamSource;
import com.linkedin.datastream.common.Datastream;
import com.linkedin.datastream.server.DatastreamTaskImpl;
import com.linkedin.datastream.server.ListBackedTransportProvider;
import com.linkedin.datastream.connectors.oracle.triggerbased.consumer.OracleChangeEvent;
import com.linkedin.datastream.server.DatastreamEventProducer;
import com.linkedin.datastream.server.DatastreamTask;
import com.linkedin.datastream.server.EventProducer;
import com.linkedin.datastream.server.api.transport.TransportProvider;
import com.linkedin.datastream.server.providers.CheckpointProvider;

import static org.mockito.Mockito.mock;


/**
 * A couple of static helper methods to be used in unit tests
 */
public class OracleConnectorTestUtils {
  private static final String SCHEMA_REGISTRY_MODE_KEY = "mode";
  private static final String SCHEMA_REGISTRY_URI_KEY = "uri";
  private static final String SCHEMA_REGISTRY_CONFIG_KEY = "kafkaSchemaRegistry";
  private static final String DB_URI_KEY = "dbUri";

  static final String DEFAULT_DB_NAME = "dbName";
  static final String DEFAULT_VIEW_NAME = "viewName";
  static final String DEFAULT_CONNECTOR_NAME = "oracle-triggerbased";

  public static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSSSSS");

  /**
   * @return The schema registry props
   */
  public static Properties getSchemaRegistryProps(String mode, String uri) {
    Properties props = new Properties();
    props.put(SCHEMA_REGISTRY_CONFIG_KEY + "." + SCHEMA_REGISTRY_MODE_KEY, mode);
    props.put(SCHEMA_REGISTRY_CONFIG_KEY + "." + SCHEMA_REGISTRY_URI_KEY, uri);
    return props;
  }

  /**
   * @return The properties needed for Oracle Connection
   */
  public static Properties getDbProps(String uri) {
    Properties props = new Properties();
    props.put(DB_URI_KEY, uri);
    return props;
  }

  /**
   * @return The properties needed for Oracle Connection for a specific database
   */
  public static Properties getDbProps(String dbName, String uri) {
    Properties props = new Properties();
    props.put(dbName + "." + DB_URI_KEY, uri);
    return props;
  }

  public static Properties getDefaultProps() {
    Properties props = new Properties();
    props.putAll(getSchemaRegistryProps("inmemory", "http://schemaUri"));
    props.putAll(getDbProps("dbName", "http://dbUri"));
    System.out.println(props);
    return props;
  }

  public static Datastream createDatastream() {
    String connString = "ListBackedTransportProvider";
    String sourceString = "oracle:/" + DEFAULT_DB_NAME + "/" + DEFAULT_VIEW_NAME;
    String schemaId = "1234";
    return createDatastream(sourceString, schemaId, connString);
  }

  /**
   * Create a generic test Datastream with transport provider and proper metadata
   *
   * @param sourceString - the connection string pointing to the source
   * @param connString - the connection string for the destination
   * @param schemaId - the schemaId of the source as registered in the Schema Registry
   *
   * @return a generic Datastream
   */
  public static Datastream createDatastream(String sourceString, String schemaId, String connString) {
    Datastream stream = new Datastream();
    stream.setName("datastream_" + connString);
    stream.setConnectorName(DEFAULT_CONNECTOR_NAME);
    stream.setTransportProviderName("test");

    StringMap metadata = new StringMap();
    metadata.put(DatastreamMetadataConstants.OWNER_KEY, "test");
    metadata.put(SchemaUtil.SCHEMA_ID_KEY, schemaId);

    DatastreamSource source = new DatastreamSource();
    source.setConnectionString(sourceString);
    stream.setMetadata(metadata);
    stream.setSource(source);

    DatastreamDestination destination = new DatastreamDestination();
    destination.setConnectionString(connString);
    stream.setDestination(destination);

    return stream;
  }

  public static DatastreamTaskImpl createDatastreamTask(Datastream stream) {
    DatastreamTaskImpl task = new DatastreamTaskImpl(Collections.singletonList(stream));
    ListBackedTransportProvider t = new ListBackedTransportProvider();
    task.setEventProducer(createEventProducer(task, t));
    return task;
  }

  public static DatastreamEventProducer createEventProducer(DatastreamTask task, TransportProvider transport) {
    CheckpointProvider cpProvider = mock(CheckpointProvider.class);
    Properties config = new Properties();
    return new EventProducer(task, transport, cpProvider, config, false);
  }

  public static List<OracleChangeEvent> generateGenericEvents(int numEvents) {
    List<OracleChangeEvent> changeEvents = new ArrayList<>();

    for (long i = 0; i < numEvents; i++) {
      OracleChangeEvent event = new OracleChangeEvent(i, System.currentTimeMillis());
      event.addRecord("getLucky", i, Types.NUMERIC);
      event.addRecord("contact", "type", Types.VARCHAR);

      changeEvents.add(event);
    }

    return changeEvents;

  }

  /**
   * Generates a list of Mock OracleChangeEvents based on the GENERIC_SCHEMA Schema
   * These objects are composed of only primitive types and avoids Structs and Collections.
   *
   * For testing with more complex events please use {@code ::generateComplexMockEvents()}
   *
   * @param numEvents The number of events to be created
   * @return a List of OracleChangeEvents to be processed by the MessageHandler
   */
  public static List<OracleChangeEvent> generateGenericMockEvents(int numEvents) {
    List<OracleChangeEvent> changeEvents = new ArrayList<>();

    for (long i = 0; i < numEvents; i++) {
      OracleChangeEvent event = createGenericChangeEvent(i);
      changeEvents.add(event);
    }

    return changeEvents;
  }

  /**
   * Generates a list of Mock OracleChangeEvents based on the COMPLEX_SCHEMA Schema
   * For testing with simpler events please use {@code ::generateGenericMockEvents()}
   *
   * @param numEvents - The number of events to be created
   * @return a List of OracleChangeEvents to be processed by the MessageHandler
   */
  public static List<OracleChangeEvent> generateComplexMockEvents(int numEvents) {
    List<OracleChangeEvent> changeEvents = new ArrayList<>();

    for (long i = 0; i < numEvents; i++) {
      OracleChangeEvent event = createComplexChangeEvent(i, MockSchema.COMPLEX_SCHEMA);
      changeEvents.add(event);
    }

    return changeEvents;
  }

  /**
   * Generates the specific OracleChangeEvent. The only required field is the membershipId because
   * that is the primaryKey
   *
   * @param i - long value indicating number event
   */
  private static OracleChangeEvent createGenericChangeEvent(long i) {
    OracleChangeEvent event = new OracleChangeEvent(i, System.currentTimeMillis());

    event.addRecord("instantCrush", i, Types.NUMERIC);
    event.addRecord("getLucky", i, Types.NUMERIC);
    event.addRecord("oneMoreTime", i, Types.NUMERIC);
    event.addRecord("somethingAboutUs", "something", Types.VARCHAR);
    event.addRecord("contact", "with", Types.CHAR);

    return event;
  }

  /**
   * Creates a complex change event. This OracleChangeEvent instance is considered complex because
   * it contains both Struct and Collection types
   *
   * @param i - for long values we use the event number
   * @param schema - Structs and Collection types need a schema
   */
  private static OracleChangeEvent createComplexChangeEvent(long i, Schema schema) {
    OracleChangeEvent event = new OracleChangeEvent(i, System.currentTimeMillis());

    // build a generic record for Geo Field
    GenericRecord geo = new GenericData.Record(schema.getField("geo").schema());
    geo.put("country", "country-string");
    geo.put("latitudeDeg", 5);

    // build an Array record for smallerCities field
    Schema smallerCitiesSchema = schema.getField("smallerCities").schema();
    GenericData.Array<GenericRecord> arr = new GenericData.Array<GenericRecord>(1, smallerCitiesSchema);

    Schema smallCitySchema = smallerCitiesSchema.getElementType();
    GenericRecord cityRecord = new GenericData.Record(smallCitySchema);
    cityRecord.put("cityId", 1L);
    cityRecord.put("cityName", "blah");

    // now place this one element inside the GenericData.Array arr
    arr.add(cityRecord);

    event.addRecord("osaka", "osaka", Types.VARCHAR);
    event.addRecord("key", i, Types.NUMERIC);
    event.addRecord("geo", geo, Types.STRUCT);
    event.addRecord("smallerCities", arr, Types.ARRAY);

    return event;
  }
}