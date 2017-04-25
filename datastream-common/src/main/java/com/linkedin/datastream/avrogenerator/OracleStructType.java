package com.linkedin.datastream.avrogenerator;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import com.google.common.base.CaseFormat;


/**
 * The OracleStructType class implements the FieldType interface in order to act
 * as a wrapper class for Oracle Struct Types. Developers can store objects in Oracle
 * that have their own sub columns. For example:
 *
 * colName: ContactInfo, Type: CONTACT_INFO
 *                             childColName1: address, Type: VARCHAR
 *                             childColName2: phoneNumber, Type: CHAR
 *
 * In this case, the fieldName is CONTACT_INFO and there is a list of
 * {@code childColumns} (which are instances of {@link OracleColumn})
 * which represent the two child columns,
 */
public class OracleStructType implements FieldType {
  private static final String AVRO_FIELD_TYPE = "record";

  private final String _schemaName;
  private final String _fieldTypeName;
  private final List<OracleColumn> _childColumns;

  public OracleStructType(String schemaName, String fieldName, List<OracleColumn> childColumns) {
    _schemaName = schemaName;
    _fieldTypeName = fieldName;
    _childColumns = Collections.unmodifiableList(childColumns);
  }

  @Override
  public String getSchemaName() {
    return _schemaName;
  }

  @Override
  public String getFieldTypeName() {
    return _fieldTypeName;
  }

  @Override
  public String getAvroFieldName() {
    return AVRO_FIELD_TYPE;
  }

  @Override
  public String toString() {
    return String.format("fieldTypeName: %s, childColumns: %s", _fieldTypeName, _childColumns);
  }

  public List<OracleColumn> getChildColumns() {
    return _childColumns;
  }

  @Override
  public String getMetadata() {
    return String.format("%s=%s;", FIELD_TYPE_NAME, getFieldTypeName());
  }

  /**
   * Construct an Avro Json in the following format for the Oracle Struct Type

   {
   "type" : [ {
   "name" : "strongerStruct",
   "type" : "record",
   "fields" : [ {
   "meta" : "dbFieldName=ALL_OF_THE_LIGHTS;dbFieldPosition=0;",
   "name" : "allOfTheLights",
   "type" : [ "string", "null" ]
   }, {
   "meta" : "dbFieldName=FLASHING_LIGHTS;dbFieldPosition=1;",
   "name" : "flashingLights",
   "type" : [ "string", "null" ]
   }, {
   "meta" : "dbFieldName=SPACESHIP;dbFieldPosition=2;",
   "name" : "spaceship",
   "type" : [ "float", "null" ]
   } ]
   }, "null" ]
   }
   */
  @Override
  public AvroJson toAvro() {
    AvroJson structAvro = new AvroJson();

    AvroJson recordType = AvroJson.recordType(getFieldTypeName(), getMetadata());
    List<Object> nullableRecordType = AvroJson.nullableType(recordType);

    structAvro.setType(nullableRecordType);

    List<Map<String, Object>> fields = new ArrayList<>();
    for (OracleColumn childCol : getChildColumns()) {
      AvroJson childAvro = childCol.toAvro();
      fields.add(childAvro.info());
    }

    recordType.setFields(fields);

    return structAvro;
  }

  /**
   * Assigning a lower case name for Oracle Struct Types
   */
  private static String lowerCamel(String fieldTypeName) {
    String name =  CaseFormat.UPPER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, fieldTypeName);
    return name + "Struct";
  }
}

