/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.avrogenerator;

import java.util.List;

import org.apache.avro.Schema;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class takes a specific SQL schema (from Oracle, MySQL, etc) and converts it into an
 * Avro Schema.
 *
 * This class is the entry point for this Library.
 */
public class SchemaGenerator {

  private static final Logger LOG = LoggerFactory.getLogger(SchemaGenerator.class);

  private String _tableName;
  private String _schemaName;
  private String _primaryKey;
  private List<String> _fields;
  private DatabaseSource _source;

  private SchemaGenerator(Builder builder) {
    _tableName = builder.tableName;
    _schemaName = builder.schemaName;
    _source = builder.databaseSource;
    _primaryKey = builder.primaryKey;
    _fields = builder.fields;
  }

  public Schema generate() throws SchemaGenerationException {
    OracleTable table = null;

    try {
      _source.initializeConnection();

      // first generate an OracleTable
      LOG.info("Generating schema for schema {}, table {}", _schemaName, _tableName);
      OracleTableFactory factory = new OracleTableFactory(_source);
      table = factory.buildOracleTable(_schemaName, _tableName, _primaryKey);
    } catch (Exception e) {
      String msg = String.format("Failed to generate schema for schema %s, table %s", _schemaName, _tableName);
      LOG.error(msg, e);
      throw new SchemaGenerationException(msg, e);
    } finally {
      if (_source != null) {
        _source.closeConnection();
        _source = null;
      }
    }

    return table.toAvro().toSchema();
  }

  /**
   * In order to have enough flexibility, SchemaGenerator.Builder uses the builder pattern
   */
  public static class Builder {

    private final String tableName;
    private final String schemaName;
    private final DatabaseSource databaseSource;

    private String primaryKey;
    private List<String> fields;

    public Builder(@NotNull String tableName, @NotNull String schemaName, @NotNull DatabaseSource databaseSource) {
      this.tableName = tableName;
      this.schemaName = schemaName;
      this.databaseSource = databaseSource;
    }

    /**
     * Overwrite the primary keys. The default is based on the resultSet of an SQL query to
     * determine the primary Keys
     */
    public Builder setPrimaryKey(String primaryKey) {
      this.primaryKey = primaryKey;
      return this;
    }

    /**
     * Overwrite the specific fields from the SQL schema which will be converted into
     * the Avro Schema. The default is every field.
     */
    public Builder setSpecificFields(List<String> fields) {
      this.fields = fields;
      return this;
    }

    /**
     * Build the AvroGenerator instance
     */
    public SchemaGenerator build() {
      return new SchemaGenerator(this);
    }
  }
}