/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.avrogenerator;

import java.util.List;


/**
 * Light weight class wrapper around a Database collection Type
 */
public class OracleCollectionType implements FieldType {
  private static final String AVRO_FIELD_NAME = "array";

  private final String _schemaName;
  private final String _fieldTypeName;
  private final FieldType _elementFieldType;

  public OracleCollectionType(String schemaName, String fieldTypeName, FieldType elementFieldType) {
    _schemaName = schemaName;
    _fieldTypeName = fieldTypeName;
    _elementFieldType = elementFieldType;
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
    return AVRO_FIELD_NAME;
  }

  public FieldType getElementFieldType() {
    return _elementFieldType;
  }

  @Override
  public String toString() {
    return String.format("CollectionType: %s, ElementType: %s", _fieldTypeName, _elementFieldType);
  }

  @Override
  public String getMetadata() {
    return String.format("%s=%s;", FIELD_TYPE_NAME, getFieldTypeName());
  }

  /**
   * Construct an Avro json in the following format for the Collection Type

   {
   "type" : [ {
   "meta" : "dbFieldTypeName=FAMOUS;",
   "name" : "famous",
   "type" : "array",
   "items" : [ {
   "name" : "GRADUATION",
   "type" : "record",
   "fields" : [ {
   "meta" : "dbFieldName=GOOD_MORNING;dbFieldPosition=0;",
   "name" : "goodMorning",
   "type" : [ "long", "null" ]
   }, {
   "meta" : "dbFieldName=FAMILY_BUSINESS;dbFieldPosition=1;",
   "name" : "familyBusiness",
   "type" : [ "string", "null" ]
   } ]
   }, "null" ]
   }, "null" ]
   }
   *
   */
  @Override
  public AvroJson toAvro() {
    AvroJson arrayRecord = new AvroJson();
    AvroJson elementTypeRecord = getElementFieldType().toAvro();

    AvroJson elementRecord = AvroJson.arrayType(getFieldTypeName(), elementTypeRecord);

    List<Object> nullableElementTypeRecord = AvroJson.nullableType(elementRecord);

    arrayRecord.setType(nullableElementTypeRecord);

    return arrayRecord;
  }
}
