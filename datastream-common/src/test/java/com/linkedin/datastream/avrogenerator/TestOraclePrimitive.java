/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.avrogenerator;

import java.util.HashMap;
import java.util.Map;

import org.jetbrains.annotations.NotNull;
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
    Assert.assertEquals(primitive.getMetadata(), "dbFieldType=NUMBER;nullable=Y;numberScale=10;numberPrecision=10;");

    primitive = new OraclePrimitiveType("NUMBER", DatabaseSource.TableMetadata.NOT_NULLABLE, -127, 0);
    Assert.assertEquals(primitive.getMetadata(), "dbFieldType=NUMBER;nullable=N;numberScale=-127;numberPrecision=0;");

    primitive = new OraclePrimitiveType("TIMESTAMP", DatabaseSource.TableMetadata.NULLABLE, -127, 0);
    Assert.assertEquals(primitive.getMetadata(), "dbFieldType=TIMESTAMP;nullable=Y;");
  }

  @Test
  public void testNumberFieldMappings() {
    // NUMBER with scale > 0 and scale <= 6 should resolve to Avro float
    OraclePrimitiveType primitive = new OraclePrimitiveType(Types.NUMBER.name(), DatabaseSource.TableMetadata.NULLABLE, 4, 0);
    Assert.assertEquals(primitive.getMetadata(), "dbFieldType=NUMBER;nullable=Y;numberScale=4;numberPrecision=0;");
    Assert.assertEquals(primitive.getAvroFieldName(), Types.FLOAT.getAvroType());

    // NUMBER with scale > 0 and scale <= 17 should resolve to Avro double
    primitive = new OraclePrimitiveType(Types.NUMBER.name(), DatabaseSource.TableMetadata.NULLABLE, 16, 0);
    Assert.assertEquals(primitive.getMetadata(), "dbFieldType=NUMBER;nullable=Y;numberScale=16;numberPrecision=0;");
    Assert.assertEquals(primitive.getAvroFieldName(), Types.DOUBLE.getAvroType());

    // NUMBER with precision > 9 and scale <= 0 should resolve to Avro long
    primitive = new OraclePrimitiveType(Types.NUMBER.name(), DatabaseSource.TableMetadata.NULLABLE, 0, 10);
    Assert.assertEquals(primitive.getMetadata(), "dbFieldType=NUMBER;nullable=Y;numberScale=0;numberPrecision=10;");
    Assert.assertEquals(primitive.getAvroFieldName(), Types.LONG.getAvroType());

    // NUMBER with precision <= 9 and scale <= 0 should resolve to Avro int
    primitive = new OraclePrimitiveType(Types.NUMBER.name(), DatabaseSource.TableMetadata.NULLABLE, 0, 3);
    Assert.assertEquals(primitive.getMetadata(), "dbFieldType=NUMBER;nullable=Y;numberScale=0;numberPrecision=3;");
    Assert.assertEquals(primitive.getAvroFieldName(), Types.INTEGER.getAvroType());

    // NUMBER with scale == 0 and scale <= 0 should resolve to Avro string
    primitive = new OraclePrimitiveType(Types.NUMBER.name(), DatabaseSource.TableMetadata.NULLABLE, 0, 0);
    Assert.assertEquals(primitive.getMetadata(), "dbFieldType=NUMBER;nullable=Y;numberScale=0;numberPrecision=0;");
    Assert.assertEquals(primitive.getAvroFieldName(), Types.NUMBER.getAvroType());

    // FLOAT should resolve to Avro float
    primitive = new OraclePrimitiveType(Types.FLOAT.name(), DatabaseSource.TableMetadata.NULLABLE, 126, 0);
    Assert.assertEquals(primitive.getMetadata(), "dbFieldType=FLOAT;nullable=Y;numberScale=126;numberPrecision=0;");
    Assert.assertEquals(primitive.getAvroFieldName(), Types.FLOAT.getAvroType());

    // DOUBLE should resolve to Avro double
    primitive = new OraclePrimitiveType(Types.DOUBLE.name(), DatabaseSource.TableMetadata.NULLABLE, 12, 0);
    Assert.assertEquals(primitive.getMetadata(), "dbFieldType=DOUBLE;nullable=Y;numberScale=12;numberPrecision=0;");
    Assert.assertEquals(primitive.getAvroFieldName(), Types.DOUBLE.getAvroType());

    // LONG should resolve to Avro long
    primitive = new OraclePrimitiveType(Types.LONG.name(), DatabaseSource.TableMetadata.NULLABLE, 0, 10);
    Assert.assertEquals(primitive.getMetadata(), "dbFieldType=LONG;nullable=Y;numberScale=0;numberPrecision=10;");
    Assert.assertEquals(primitive.getAvroFieldName(), Types.LONG.getAvroType());

    // INTEGER should resolve to Avro integer
    primitive = new OraclePrimitiveType(Types.INTEGER.name(), DatabaseSource.TableMetadata.NULLABLE, 0, 7);
    Assert.assertEquals(primitive.getMetadata(), "dbFieldType=INTEGER;nullable=Y;numberScale=0;numberPrecision=7;");
    Assert.assertEquals(primitive.getAvroFieldName(), Types.INTEGER.getAvroType());
  }

  @Test
  public void testFieldMappings() {
    OraclePrimitiveType primitive;
    for (Types type : Types.values()) {
      // skip NUMBER fields as they require separate validation for scale, precision, and mapping logic
      if (OraclePrimitiveType.NUMBER_CLASSIFICATION.contains(type.name())) {
        continue;
      }

      primitive = new OraclePrimitiveType(type.name(), DatabaseSource.TableMetadata.NULLABLE, 0, 0);
      Assert.assertEquals(primitive.getAvroFieldName(), type.getAvroType());
      Assert.assertEquals(primitive.getMetadata(), "dbFieldType=" + type.name() + ";nullable=Y;");
    }
  }

  @Test
  public void testToAvro() {
    OraclePrimitiveType primitive = new OraclePrimitiveType("NUMBER", DatabaseSource.TableMetadata.NULLABLE, 0, 10);
    Map<String, Object> info = primitive.toAvro().info();
    String[] types = (String[]) info.get("type");

    Assert.assertEquals(types[0], "null");
    Assert.assertEquals(types[1], "long");
  }

  @Test
  public void testTableMetadataWithMetadataMap() {
    DatabaseSource.TableMetadata tableMetadata =
        new TestTableMetadata("NUMBER", "VALUE", DatabaseSource.TableMetadata.NOT_NULLABLE, 0, 0, "long");
    OraclePrimitiveType primitive =
        new OraclePrimitiveType("NUMBER", DatabaseSource.TableMetadata.NOT_NULLABLE, 0, 0, tableMetadata);
    Assert.assertEquals(primitive.getMetadata(),
        "dbFieldType=NUMBER;nullable=N;numberScale=0;numberPrecision=0;extraMetaField=long;");
  }

  public class TestTableMetadata extends DatabaseSource.TableMetadata {
    public static final String EXTRA_META_FIELD = "extraMetaField";
    private final Map<String, String> _metaMap;

    public TestTableMetadata(@NotNull String colTypeName, @NotNull String colName, @NotNull String nullable,
        int precision, int scale, String extraMetaField) {
      super(colTypeName, colName, nullable, precision, scale);
      _metaMap = new HashMap<>();
      _metaMap.put(EXTRA_META_FIELD, extraMetaField);
    }

    @Override
    public Map<String, String> getMetadataMap() {
      return _metaMap;
    }
  }
}
