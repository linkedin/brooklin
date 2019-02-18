/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.avrogenerator;

import com.google.common.base.CaseFormat;

/**
 * The OracleColumn class is a simple light weight class that represents a column in a Database table.
 * The class {@link OracleTable} are composed of multiple instances of {@link OracleColumn}.
 * While {@link FieldType} classes hold information about the Database types
 * (acting as wrappers for CHAR, VARCHAR, etc), the {@link OracleColumn} class is a wrapper a specific
 * column in a Database table. Therefore, information about the columnName is stored in member variables
 * of {@link OracleColumn} instances.
 *
 * Note: {@link OracleStructType} are {@link FieldType}, but since Structs can be seen as sub tables (or user
 * defined table with in a field), {@link OracleStructType} are also composed of {@link OracleColumn} instances
 *
                                  ┌─────────────────────┐
                              ┌──▶│ OraclePrimitiveType │
 ┌──────────────────┐         │   └─────────────────────┘
 │   OracleColumn   │         │
 ├──────────────────┤         │   ┌─────────────────────┐
 │  FieldType ──────┼─────────┼──▶│OracleCollectionType │
 │                  │         │   └─────────────────────┘
 │  colName         │         │
 │  colPosition     │         │   ┌─────────────────────┐
 │                  │         └──▶│  OracleStructType   │
 └──────────────────┘             └─────────────────────┘
 */
public class OracleColumn {
  static final String COL_NAME = "dbFieldName";
  static final String COL_POSITION = "dbFieldPosition";

  private final String _colName;
  private final FieldType _fieldType;
  private final int _colPosition;

  public OracleColumn(String colName, FieldType fieldTypeInfo, int colPosition) {
    _colName = colName;
    _fieldType = fieldTypeInfo;
    _colPosition = colPosition;
  }

  /**
   * @return name of the field
   */
  public String getColName() {
    return _colName;
  }

  /**
   * @return the position (index) of this field; note that indexes are always zero based even though rs.getObject(...) is 1 based!
   */
  public int getColPosition() {
    return _colPosition;
  }

  /**
   * @return return the FieldType wrapper class for the Database Column
   */
  public FieldType getFieldType() {
    return _fieldType;
  }

  /**
   * Each field in a Schema Record is composed to look like the following:
   *  {
   "meta" : "dbFieldName=WAVES;dbFieldPosition=1;",
   "name" : "waves",
   "type" : [ "string", "null" ]
   }
   *
   * The above example is when we have a primitive type as the {@code _fieldType},
   * With more complex structures such as Collections and Structs we see the {@code type}
   * field replaced by the associated {@code _fieldType.toAvro()}
   */
  public AvroJson toAvro() {
    AvroJson typeAvro = getFieldType().toAvro();
    typeAvro.nullDefault();
    typeAvro.setName(lowerCamel(getColName()));
    typeAvro.setMeta(buildMetadata(getColName(), getColPosition(), getFieldType().getMetadata()));

    return typeAvro;
  }

  private static String buildMetadata(String dbColName, int position, String fieldTypeMetadata) {
    StringBuilder meta = new StringBuilder();

    meta.append(String.format("%s=%s;", COL_NAME, dbColName));
    meta.append(String.format("%s=%s;", COL_POSITION, position));
    meta.append(fieldTypeMetadata);

    return meta.toString();
  }

  /**
   * colName's in avro should be in lower camel case format
   */
  private static String lowerCamel(String colName) {
    return CaseFormat.UPPER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, colName);
  }

  @Override
  public String toString() {
    return String.format("Field: %s, FieldType: %s", _colName, _fieldType);
  }
}