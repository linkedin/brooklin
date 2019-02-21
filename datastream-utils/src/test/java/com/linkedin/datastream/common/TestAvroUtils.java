package com.linkedin.datastream.common;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.testng.Assert;
import org.testng.annotations.Test;

import static com.linkedin.datastream.common.AvroUtils.encodeAvroIndexedRecordAsJson;


public class TestAvroUtils {

  final static public String SCHEMA_STRING =
      "{ \"type\": \"record\","
      + "\"namespace\": \"com.example\","
      + "\"name\": \"FullName\","
      + "\"fields\": [ "
      + "{ \"name\": \"first\", \"type\": \"string\" } "
      + "]}";

  @Test
  public void testEncodeAvroIndexedRecordAsJson() throws Exception {
    String expectedValue = "name123";
    Schema schema = Schema.parse(SCHEMA_STRING);
    GenericRecord record = new GenericData.Record(schema);
    record.put("first", expectedValue);
    String json = new String(encodeAvroIndexedRecordAsJson(schema, record));
    Assert.assertTrue(json.contains(expectedValue));
  }

  @Test
  public void testDecodeAvroAsJson() throws Exception {
    String expectedValue = "{\"first\":\"name123\"}";
    Schema schema = Schema.parse(SCHEMA_STRING);
    GenericRecord record = AvroUtils.decodeJsonAsAvroGenericRecord(schema, expectedValue.getBytes(), null);
    String encodedValue = new String(AvroUtils.encodeAvroIndexedRecordAsJson(schema, record));
    Assert.assertEquals(expectedValue, encodedValue);
  }
}
