package com.linkedin.datastream.connectors.oracle.triggerbased.consumer;

import java.math.BigDecimal;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Struct;
import java.sql.Timestamp;
import java.sql.ResultSet;
import java.sql.Types;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import org.apache.avro.generic.GenericData;
import org.mockito.Mockito;
import org.testng.Assert;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.testng.annotations.Test;

import com.linkedin.datastream.common.DatastreamRuntimeException;
import com.linkedin.datastream.connectors.oracle.triggerbased.MockSchema;

@Test
public class TestOracleTableReader {
  private static final String COL_NAME = "lowLights";

  @Test
  public void testGetChildSchemaValidUnion() {
    List<Type> acceptable = new ArrayList<>();
    acceptable.add(Type.LONG);

    // expect to not throw error
    Schema childSchema = OracleTableReader.getChildSchema(MockSchema.GENERIC_SCHEMA, "getLucky", acceptable);
  }

  @Test
  public void testGetChildSchemaValid() {
    Schema schema =
        org.apache.avro.Schema.parse("{\"name\":\"Feedback\",\"type\":\"record\",\"fields\":[{\"name\":\"lowLights\",\"type\":\"long\"}]}");

    List<Type> acceptable = new ArrayList<>();
    acceptable.add(Type.LONG);

    // expect to not throw error
    Schema childSchema = OracleTableReader.getChildSchema(schema, COL_NAME, acceptable);
  }

  @Test(expectedExceptions = DatastreamRuntimeException.class)
  public void testGetChildSchemaUnacceptableType() {
    Schema schema =
        org.apache.avro.Schema.parse("{\"name\":\"Feedback\",\"type\":\"record\",\"fields\":[{\"name\":\"lowLights\",\"type\":\"long\"}]}");

    List<Type> acceptable = new ArrayList<>();
    acceptable.add(Type.STRING);

    // expect to throw error
    Schema childSchema = OracleTableReader.getChildSchema(schema, COL_NAME, acceptable);
  }

  @Test
  public void testSqlObjectToAvro() throws SQLException {
    String dbValue = "value";
    Object object = OracleTableReader.sqlObjectToAvro(dbValue, "contact", MockSchema.GENERIC_SCHEMA);

    Assert.assertEquals(object, dbValue);
    Assert.assertTrue(object instanceof String);
  }

  @Test
  public void testSqlObjectToAvroNumeric() throws SQLException {
    BigDecimal dbValue = new BigDecimal(123L);

    // even though the argument is BigDecimal, we should convert to an int because
    // its defined as an int in the schema
    Object object = OracleTableReader.sqlObjectToAvro(dbValue, "getLucky", MockSchema.GENERIC_SCHEMA);
    Assert.assertEquals(object, dbValue.longValue());
    Assert.assertTrue(object instanceof Long);
  }

  @Test
  public void testSqlObjectToAvroTimestamp() throws SQLException {
    Timestamp ts = new Timestamp(123L);

    Object object = OracleTableReader.sqlObjectToAvro(ts, "digitalLove", MockSchema.GENERIC_SCHEMA);
    Assert.assertEquals(object, ts.getTime());
    Assert.assertTrue(object instanceof Long);
  }

  @Test(expectedExceptions = DatastreamRuntimeException.class)
  public void testSqlObjectToAvroUnexpectedType() throws SQLException {
    Duration duration = Duration.ofMillis(10L);

    // we dont expect Duration types from ResultSets
    Object object = OracleTableReader.sqlObjectToAvro(duration, "getLucky", MockSchema.GENERIC_SCHEMA);
  }

  @Test(expectedExceptions = DatastreamRuntimeException.class)
  public void testNullUnion() {
    // ensure we handle the situation where the only types in a schema  are only one item
    Schema schema =
        Schema.parse("{\"name\":\"Feedback\",\"type\":\"record\",\"fields\":[{\"name\":\"lowLights\",\"type\": [\"null\"]}]}");

    List<Type> list = new ArrayList<>();
    list.add(Type.STRING);

    Schema child = OracleTableReader.getChildSchema(schema, "lowLights", list);
  }

  @Test
  public void testGenerateEvent() throws SQLException {
    ResultSet rs = Mockito.mock(ResultSet.class);
    ResultSetMetaData rsmd = Mockito.mock(ResultSetMetaData.class);

    Mockito.when(rs.next()).thenReturn(true).thenReturn(true).thenReturn(false);
    Mockito.when(rs.getMetaData()).thenReturn(rsmd);

    Mockito.when(rsmd.getColumnCount()).thenReturn(4);

    Mockito.when(rsmd.getColumnType(3)).thenReturn(Types.NUMERIC);
    Mockito.when(rsmd.getColumnType(4)).thenReturn(Types.NUMERIC);
    Mockito.when(rsmd.getColumnName(3)).thenReturn("TXN");
    Mockito.when(rsmd.getColumnName(4)).thenReturn("GET_LUCKY");

    Mockito.when(rs.getObject(3)).thenReturn(BigDecimal.valueOf(1));
    Mockito.when(rs.getObject(4)).thenReturn(BigDecimal.valueOf(10L));

    // do not throw error
    OracleChangeEvent event =
        OracleTableReader.generateEvent(rs, MockSchema.GENERIC_SCHEMA, 10L, 100L);

    Assert.assertEquals(event.getScn(), 10L);
    Assert.assertEquals(event.getSourceTimestamp(), 100L);

    List<OracleChangeEvent.Record> records = event.getRecords();
    OracleChangeEvent.Record txnRecord = records.get(0);
    OracleChangeEvent.Record keyRecord = records.get(1);

    Assert.assertEquals(txnRecord.getColName(), "txn");
    Assert.assertEquals(txnRecord.getValue(), 1L);
    Assert.assertEquals(txnRecord.getSqlType(), Types.NUMERIC);
    Assert.assertEquals(keyRecord.getColName(), "getLucky");
    Assert.assertEquals(keyRecord.getValue(), 10L);
    Assert.assertEquals(keyRecord.getSqlType(), Types.NUMERIC);
  }

  @Test(expectedExceptions = DatastreamRuntimeException.class)
  public void testGenerateEventInvalidType() throws SQLException {
    ResultSet rs = Mockito.mock(ResultSet.class);
    ResultSetMetaData rsmd = Mockito.mock(ResultSetMetaData.class);

    Mockito.when(rs.next()).thenReturn(true).thenReturn(true).thenReturn(false);
    Mockito.when(rs.getMetaData()).thenReturn(rsmd);

    Mockito.when(rsmd.getColumnCount()).thenReturn(4);

    Mockito.when(rsmd.getColumnType(3)).thenReturn(Types.NUMERIC);
    Mockito.when(rsmd.getColumnType(4)).thenReturn(Types.NUMERIC);
    Mockito.when(rsmd.getColumnName(3)).thenReturn("TXN");
    Mockito.when(rsmd.getColumnName(4)).thenReturn("GET_LUCKY");

    // Instant instances should not be converted
    Mockito.when(rs.getObject(3)).thenReturn(Instant.now());
    Mockito.when(rs.getObject(4)).thenReturn(BigDecimal.valueOf(10L));

    OracleChangeEvent event =
        OracleTableReader.generateEvent(rs, MockSchema.GENERIC_SCHEMA, 10L, 100L);
  }

  @Test(expectedExceptions = DatastreamRuntimeException.class)
  public void testGenerateEventInvalidCount() throws SQLException {
    ResultSet rs = Mockito.mock(ResultSet.class);
    ResultSetMetaData rsmd = Mockito.mock(ResultSetMetaData.class);

    Mockito.when(rs.next()).thenReturn(true).thenReturn(true).thenReturn(false);
    Mockito.when(rs.getMetaData()).thenReturn(rsmd);

    Mockito.when(rsmd.getColumnCount()).thenReturn(2);

    OracleChangeEvent event =
        OracleTableReader.generateEvent(rs, MockSchema.GENERIC_SCHEMA, 10L, 100L);
  }

  public void testStructGeneration() throws Exception {
    Object[] objects = new Object[4];
    String country = "a";
    String regionCode = "b";
    BigDecimal latitude  = new BigDecimal(11.0F);
    BigDecimal longitude = new BigDecimal(12.0F);

    objects[0] = country;
    objects[1] = regionCode;
    objects[2] = latitude;
    objects[3] = longitude;

    Struct struct = Mockito.mock(Struct.class);
    Mockito.when(struct.getAttributes()).thenReturn(objects);
    Schema structSchema = MockSchema.COMPLEX_SCHEMA.getField("geo").schema();

    GenericData.Record record =
        (GenericData.Record) OracleTableReader.sqlObjectToAvro(struct, COL_NAME, structSchema);

    Assert.assertEquals(record.get("country"), country);
    Assert.assertEquals(record.get("geoPlaceMaskCode"), regionCode);
    Assert.assertEquals(record.get("latitudeDeg"), latitude.floatValue());
    Assert.assertEquals(record.get("longitudeDeg"), longitude.floatValue());
  }
}
