package com.linkedin.datastream.avrogenerator;

import java.util.Map;
import org.testng.Assert;
import org.testng.annotations.Test;


@Test
public class TestOraclePrimitive {

  @Test
  public void testConstructor() {
    OraclePrimitiveType primitive = new OraclePrimitiveType("VARCHAR2", 0, 0);
    Assert.assertEquals(primitive.getAvroFieldName(), "string");
    Assert.assertEquals(primitive.getFieldTypeName(), "VARCHAR2");
    Assert.assertEquals(primitive.getSchemaName(), null);

    primitive = new OraclePrimitiveType("CLOB", 0, 0);
    Assert.assertEquals(primitive.getAvroFieldName(), "string");

    primitive = new OraclePrimitiveType("DATE", 0, 0);
    Assert.assertEquals(primitive.getAvroFieldName(), "long");

    primitive = new OraclePrimitiveType("BLOB", 0, 0);
    Assert.assertEquals(primitive.getAvroFieldName(), "bytes");

    primitive = new OraclePrimitiveType("NUMBER", 1, 0);
    Assert.assertEquals(primitive.getAvroFieldName(), "float");

    primitive = new OraclePrimitiveType("NUMBER", 7, 0);
    Assert.assertEquals(primitive.getAvroFieldName(), "double");

    primitive = new OraclePrimitiveType("NUMBER", 0, 10);
    Assert.assertEquals(primitive.getAvroFieldName(), "long");

    primitive = new OraclePrimitiveType("NUMBER", 0, 1);
    Assert.assertEquals(primitive.getAvroFieldName(), "int");

    primitive = new OraclePrimitiveType("NUMBER", -127, 0);
    Assert.assertEquals(primitive.getAvroFieldName(), "string");

    primitive = new OraclePrimitiveType("NUMBER", 0, 0);
    Assert.assertEquals(primitive.getAvroFieldName(), "string");
  }

  @Test(expectedExceptions = RuntimeException.class)
  public void testInvalidScale() {
    OraclePrimitiveType primitive = new OraclePrimitiveType("NUMBER", 18, 0);
  }

  @Test
  public void testGetMetadata() {
    OraclePrimitiveType primitive = new OraclePrimitiveType("VARCHAR2", 0, 0);
    Assert.assertEquals(primitive.getMetadata(), "dbFieldType=VARCHAR2;");

    primitive = new OraclePrimitiveType("NUMBER", 10, 10);
    Assert.assertEquals(primitive.getMetadata(), "dbFieldType=DOUBLE;numberScale=10;numberPrecision=10;");

    primitive = new OraclePrimitiveType("NUMBER", -127, 0);
    Assert.assertEquals(primitive.getMetadata(), "dbFieldType=NUMBER;numberScale=-127;numberPrecision=0;");

    primitive = new OraclePrimitiveType("TIMESTAMP", -127, 0);
    Assert.assertEquals(primitive.getMetadata(), "dbFieldType=TIMESTAMP;");
  }

  @Test
  public void testToAvro() {
    OraclePrimitiveType primitive = new OraclePrimitiveType("NUMBER", 0, 10);
    Map<String, Object> info = primitive.toAvro().info();
    String[] types = (String[]) info.get("type");


    Assert.assertEquals(types[0], "null");
    Assert.assertEquals(types[1], "long");
  }
}
