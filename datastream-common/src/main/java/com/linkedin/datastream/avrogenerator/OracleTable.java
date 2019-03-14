/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.avrogenerator;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;


/**
 * The {@code OracleTable} class is the top-level wrapper for the entire table in the Database
 * It is composed of multiple {@code OracleColumn} instances which associate to each Column in the
 * Database Table.
 *
 * Calling {@code #toAvro()} from an instance of {@code OracleTable} will generate an Avro Schema
 * for the entire table (in Json form). A secondary call to {@code AvroJson#toSchema()} will generate
 * the final Avro Schema
 *

 OracleTable
 ┌─────────────────────┐
 │      ┌─────────────┐│
 │      │OracleColumn ││
 │      └─────────────┘│
 │      ┌─────────────┐│
 │      │OracleColumn ││
 │      └─────────────┘│
 │      ┌─────────────┐│
 │      │OracleColumn ││
 │      └─────────────┘│
 └─────────────────────┘

 */
public class OracleTable {
  /* The key for the table name stored in metadata */
  private static final String TABLE_NAME_KEY = "dbTableName";

  /* The key for Primary Key for metadata */
  private static final String PRIMARY_KEY = "pk";

  /* prefix for table namespace */
  private static final String NAMESPACE_PREFIX = "com.linkedin.events.";

  private final String _tableName;
  private final String _primaryKey;
  private final String _schemaName;
  private final List<OracleColumn> _childColumns;

  public OracleTable(@NotNull String tableName, @NotNull String schemaName, @NotNull List<OracleColumn> childColumns,
      @Nullable String primaryKey) {
    _tableName = tableName;
    _childColumns = childColumns;
    _primaryKey = primaryKey;
    _schemaName = schemaName;
  }

  public String getPrimaryKey() {
    return _primaryKey;
  }

  private String getMetadata() {
    StringBuilder meta = new StringBuilder();

    meta.append(String.format("%s=%s;", TABLE_NAME_KEY, _tableName));
    meta.append(String.format("%s=%s;", PRIMARY_KEY, _primaryKey));

    return meta.toString();
  }

  public AvroJson toAvro() {
    AvroJson tableAvro = AvroJson.recordType(_tableName, getMetadata());

    List<Map<String, Object>> fields = new ArrayList<>();
    for (OracleColumn childCol : _childColumns) {
      AvroJson childAvro = childCol.toAvro();
      fields.add(childAvro.info());
    }

    tableAvro.setFields(fields);
    tableAvro.setDoc(buildDoc(_tableName));
    tableAvro.setNamespace(buildNamespace(_schemaName));

    return tableAvro;
  }

  @Override
  public String toString() {
    return String.format("tableName: %s, PrimaryKey: %s", _tableName, _primaryKey);
  }

  private static String buildDoc(String tableName) {
    SimpleDateFormat df = new SimpleDateFormat("MMM dd, yyyy hh:mm:ss a zzz");
    return String.format("Auto-generated Avro schema for %s. Generated at %s", tableName,
        df.format(new Date(System.currentTimeMillis())));
  }

  private static String buildNamespace(String schemaName) {
    return NAMESPACE_PREFIX + schemaName.toLowerCase();
  }
}
