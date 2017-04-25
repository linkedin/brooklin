package com.linkedin.datastream.avrogenerator;

import java.sql.SQLException;
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
    _primaryKey = builder._primaryKey;
    _fields = builder._fields;
    _source = builder.databaseSource;
  }

  public Schema generate() throws SchemaGenerationException {
    initializeClient();

    // first generate an OracleTable
    OracleTableFactory factory = new OracleTableFactory(_source);
    OracleTable table = null;

    try {
      table = factory.buildOracleTable(_schemaName, _tableName, _primaryKey);
    } catch (SQLException | SchemaGenerationException e) {
      throw new SchemaGenerationException("Failed to build OracleTable", e);
    } finally {
      closeClient();
    }

    return table.toAvro().toSchema();
  }

  private void closeClient() {
    if (_source == null) {
      return;
    }

    _source.closeConnection();
    _source = null;
  }

  private void initializeClient() {
    try {
      _source.initializeConnection();
    } catch (SQLException e) {
      LOG.error("Failed to initialize connection");
      throw new RuntimeException(e);
    }
  }

  /**
   * In order to have enough flexibility, SchemaGenerator.Builder uses the builder pattern
   */
  public static class Builder {

    private final String tableName;
    private final String schemaName;
    private final DatabaseSource databaseSource;


    private String _primaryKey;
    private List<String> _fields;
    private DatabaseSource _databaseSource;

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
      _primaryKey = primaryKey;
      return this;
    }

    /**
     * Overwrite the specific fields from the SQL schema which will be converted into
     * the Avro Schema. The default is every field.
     */
    public Builder setSpecificFields(List<String> fields) {
      _fields = fields;
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