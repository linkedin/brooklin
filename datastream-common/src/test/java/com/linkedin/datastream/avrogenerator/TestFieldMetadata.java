package com.linkedin.datastream.avrogenerator;

import java.util.Map;
import java.util.Optional;

import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Unit tests for FieldMetadata
 */
@Test
public class TestFieldMetadata {

  @Test
  public void testFromStringPositive() throws Exception {
    String meta = "dbFieldName=AUTHOR;dbFieldPosition=26;dbFieldType=NVARCHAR2;";

    FieldMetadata fieldMetadata = FieldMetadata.fromString(meta);
    Assert.assertEquals(fieldMetadata.getDbFieldName(), "AUTHOR", "Incorrectly parsed dbFieldName");
    Assert.assertEquals(fieldMetadata.getDbFieldPosition(), 26, "Incorrectly parsed dbFieldPosition");
    Assert.assertEquals(fieldMetadata.getDbFieldType(), Types.NVARCHAR2, "Incorrectly parsed dbFieldType");
    Assert.assertEquals(fieldMetadata.getNumberPrecision(), Optional.empty());
    Assert.assertEquals(fieldMetadata.getNumberScale(), Optional.empty());

    // with precision and scale
    meta = "dbFieldName=WEIGHT;dbFieldPosition=1;dbFieldType=FLOAT;numberScale=2;numberPrecision=3;";
    fieldMetadata = FieldMetadata.fromString(meta);
    Assert.assertEquals(fieldMetadata.getDbFieldName(), "WEIGHT", "Incorrectly parsed dbFieldName");
    Assert.assertEquals(fieldMetadata.getDbFieldPosition(), 1, "Incorrectly parsed dbFieldPosition");
    Assert.assertEquals(fieldMetadata.getDbFieldType(), Types.FLOAT, "Incorrectly parsed dbFieldType");
    Assert.assertEquals(fieldMetadata.getNumberPrecision(), Optional.of(3), "Incorrectly parsed numberPrecision");
    Assert.assertEquals(fieldMetadata.getNumberScale(), Optional.of(2), "Incorrectly parsed numberScale");

    meta = "dbFieldName=ARTICLE;dbFieldPosition=1;dbFieldType=LONG RAW;";
    fieldMetadata = FieldMetadata.fromString(meta);
    Assert.assertEquals(fieldMetadata.getDbFieldType(), Types.LONG_RAW, "Incorrectly parsed dbFieldType");

    // with extra metadata
    meta = "dbFieldName=WEIGHT;dbFieldPosition=1;dbFieldType=FLOAT;numberScale=2;numberPrecision=3;extraMetadataField=long";
    fieldMetadata = FieldMetadata.fromString(meta);
    Assert.assertEquals(fieldMetadata.getDbFieldName(), "WEIGHT", "Incorrectly parsed dbFieldName");
    Map<String, String> map = FieldMetadata.parseMetadata(meta);
    Assert.assertEquals("WEIGHT", map.get("dbFieldName"));
    Assert.assertEquals("long", map.get("extraMetadataField"));
  }

  @Test
  public void testFromStringNegative() throws Exception {
    String meta;
    FieldMetadata fieldMetadata;

    // missing field name
    try {
      meta = "dbFieldPosition=1;dbFieldType=FLOAT;numberScale=2;numberPrecision=3;";
      fieldMetadata = FieldMetadata.fromString(meta);
      Assert.fail("Parsing field metadata from string should have failed due to missing field name");
    } catch (Exception e) {
      Assert.assertTrue(e instanceof NullPointerException);
    }

    // missing field position
    try {
      meta = "dbFieldName=AUTHOR;dbFieldType=FLOAT;numberScale=2;numberPrecision=3;";
      fieldMetadata = FieldMetadata.fromString(meta);
      Assert.fail("Parsing field metadata from string should have failed due to missing field position");
    } catch (Exception e) {
      Assert.assertTrue(e instanceof NullPointerException);
    }

    // missing field type
    try {
      meta = "dbFieldName=AUTHOR;dbFieldPosition=26;";
      fieldMetadata = FieldMetadata.fromString(meta);
      Assert.fail("Parsing field metadata from string should have failed due to missing field type");
    } catch (Exception e) {
      Assert.assertTrue(e instanceof NullPointerException);
    }

    // invalid delimiter
    try {
      meta = "dbFieldName=AUTHOR,dbFieldPosition=26,dbFieldType=NVARCHAR2;";
      fieldMetadata = FieldMetadata.fromString(meta);
      Assert.fail("Parsing field metadata from string should have failed due to invalid delimiter");
    } catch (Exception e) {
      Assert.assertTrue(e instanceof IllegalArgumentException);
    }

    try {
      meta = "dbFieldName:AUTHOR;dbFieldPosition:26;dbFieldType:NVARCHAR2;";
      fieldMetadata = FieldMetadata.fromString(meta);
      Assert.fail("Parsing field metadata from string should have failed due to invalid delimiter");
    } catch (Exception e) {
      Assert.assertTrue(e instanceof IllegalArgumentException);
    }
  }
}
