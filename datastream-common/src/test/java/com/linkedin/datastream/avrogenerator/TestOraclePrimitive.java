package com.linkedin.datastream.avrogenerator;

import java.util.Map;

import org.testng.Assert;
import org.testng.annotations.Test;


@Test
public class TestOraclePrimitive {

  @Test
  public void testConstructor() {
    OraclePrimitiveType primitive = new OraclePrimitiveType("VARCHAR2", DatabaseSource.TableMetadata.NULLABLE, 0, 0);
    Assert.assertEquals(primitive.getAvroFieldName(), "string");
    Assert.assertEquals(primitive.getFieldTypeName(), "VARCHAR2");
    Assert.assertEquals(primitive.getSchemaName(), null);

    primitive = new OraclePrimitiveType("CLOB", DatabaseSource.TableMetadata.NULLABLE, 0, 0);
    Assert.assertEquals(primitive.getAvroFieldName(), "string");

    primitive = new OraclePrimitiveType("DATE", DatabaseSource.TableMetadata.NULLABLE, 0, 0);
    Assert.assertEquals(primitive.getAvroFieldName(), "long");

    primitive = new OraclePrimitiveType("BLOB", DatabaseSource.TableMetadata.NULLABLE, 0, 0);
    Assert.assertEquals(primitive.getAvroFieldName(), "bytes");

    primitive = new OraclePrimitiveType("NUMBER", DatabaseSource.TableMetadata.NULLABLE, 1, 0);
    Assert.assertEquals(primitive.getAvroFieldName(), "float");

    primitive = new OraclePrimitiveType("NUMBER", DatabaseSource.TableMetadata.NULLABLE, 7, 0);
    Assert.assertEquals(primitive.getAvroFieldName(), "double");

    primitive = new OraclePrimitiveType("NUMBER", DatabaseSource.TableMetadata.NULLABLE, 0, 10);
    Assert.assertEquals(primitive.getAvroFieldName(), "long");

    primitive = new OraclePrimitiveType("NUMBER", DatabaseSource.TableMetadata.NULLABLE, 0, 1);
    Assert.assertEquals(primitive.getAvroFieldName(), "int");

    primitive = new OraclePrimitiveType("NUMBER", DatabaseSource.TableMetadata.NULLABLE, -127, 0);
    Assert.assertEquals(primitive.getAvroFieldName(), "string");

    primitive = new OraclePrimitiveType("NUMBER", DatabaseSource.TableMetadata.NULLABLE, 0, 0);
    Assert.assertEquals(primitive.getAvroFieldName(), "string");
  }

  @Test(expectedExceptions = RuntimeException.class)
  public void testInvalidScale() {
    OraclePrimitiveType primitive = new OraclePrimitiveType("NUMBER", DatabaseSource.TableMetadata.NULLABLE, 18, 0);
  }

  @Test
  public void testGetMetadata() {
    OraclePrimitiveType primitive = new OraclePrimitiveType("VARCHAR2", DatabaseSource.TableMetadata.NULLABLE, 0, 0);
    Assert.assertEquals(primitive.getMetadata(), "dbFieldType=VARCHAR2;nullable=Y;");

    primitive = new OraclePrimitiveType("NUMBER", DatabaseSource.TableMetadata.NULLABLE, 10, 10);
    Assert.assertEquals(primitive.getMetadata(), "dbFieldType=DOUBLE;nullable=Y;numberScale=10;numberPrecision=10;");

    primitive = new OraclePrimitiveType("NUMBER", DatabaseSource.TableMetadata.NOT_NULLABLE, -127, 0);
    Assert.assertEquals(primitive.getMetadata(), "dbFieldType=NUMBER;nullable=N;numberScale=-127;numberPrecision=0;");

    primitive = new OraclePrimitiveType("TIMESTAMP", DatabaseSource.TableMetadata.NULLABLE, -127, 0);
    Assert.assertEquals(primitive.getMetadata(), "dbFieldType=TIMESTAMP;nullable=Y;");
  }

  @Test
  public void testToAvro() {
    OraclePrimitiveType primitive = new OraclePrimitiveType("NUMBER", DatabaseSource.TableMetadata.NULLABLE, 0, 10);
    Map<String, Object> info = primitive.toAvro().info();
    String[] types = (String[]) info.get("type");

    Assert.assertEquals(types[0], "null");
    Assert.assertEquals(types[1], "long");
  }
}
