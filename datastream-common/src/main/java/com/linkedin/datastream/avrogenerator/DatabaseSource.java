package com.linkedin.datastream.avrogenerator;

import java.sql.SQLException;
import java.util.List;

import org.jetbrains.annotations.NotNull;


/**
 * The Schema generator operates by making queries to the targeted database in order to
 * grab information about the Database Field Types.
 *
 * Implementations of this interface are responsible for all communication to the Database.
 */
public abstract class DatabaseSource {

  /**
   * Initialize connection to the database
   */
  protected void initializeConnection() throws SQLException {
    // default implementation is no-op
  }

  /**
   * Close connection to the database
   */
  protected void closeConnection() {
    // default implementation is no-op
  }

  /**
   * Determine if the arguments points to a table or view in the Database
   * @return true is exists, false otherwise
   */
  protected abstract boolean isTable(String schemaName, String tableName);

  /**
   * Determine if the arguments point to a Collection type in the table
   * @return true is collection, false otherwise
   */
  protected abstract boolean isCollection(String schemaName, String fieldTypeName) throws SQLException;

  /**
   * Determine if the arguments point to Struct Type in the table
   * @return true if Struct, false otherwise
   */
  protected abstract boolean isStruct(String schemaName, String fieldTypeName) throws SQLException;

  /**
   * Determine if the fieldTypeName is one of the primitive types of the Database
   * @return true if primitive
   */
  protected abstract boolean isPrimitive(String fieldTypeName) throws SQLException;

  /**
   * Retrieve the table Metadata containing information such as all of Column names
   * and their types.
   */
  protected abstract List<TableMetadata> getTableMetadata(String schemaName, String tableName) throws SQLException;

  /**
   * Retrieve the metadata of the Struct type. Contains information about the child columns that are
   * associated with this Struct Type
   */
  protected abstract List<StructMetadata> getStructMetadata(String schemaName, String fieldTypeName) throws SQLException;

  /**
   * Retrieve the metadata of the Collection Type. Contains information about the elements of the
   * Array Type
   */
  protected abstract CollectionMetadata getCollectionMetadata(String schemaName, String fieldTypeName) throws SQLException;

  /**
   * Create and run a query to get all primary keys from the Database table/view
   */
  protected abstract List<String> getPrimaryKeyFields(String tableName) throws SQLException, SchemaGenerationException;

  /**
   * Create and execute a query to get all the fields from the Database table/view
   */
  protected abstract List<String> getAllFields(String tableName, String dbName) throws SQLException;


  protected static class CollectionMetadata {
    private String _fieldName;
    private String _schemaName;
    private int _precision;
    private int _scale;

    protected CollectionMetadata(@NotNull String schemaName, @NotNull  String fieldName, int precision, int scale) {
      _fieldName = fieldName;
      _schemaName = schemaName;
      _precision = precision;
      _scale = scale;
    }

    protected String getElementFieldTypeName() {
      return _fieldName;
    }

    protected String getElementSchemaName() {
      return _schemaName;
    }

    protected int getElementPrecision() {
      return _precision;
    }

    protected int getElementScale() {
      return _scale;
    }
  }


  protected static class StructMetadata {
    private String _fieldTypeName;
    private String _schemaName;
    private String _colName;
    private int _precision;
    private int _scale;


    protected StructMetadata(String schemaName, @NotNull String fieldTypeName, @NotNull String colName, int precision, int scale) {
      _fieldTypeName = fieldTypeName;
      _schemaName = schemaName;
      _colName = colName;
      _precision = precision;
      _scale = scale;
    }

    protected String getSchemaName() {
      return _schemaName;
    }

    protected String getFieldTypeName() {
      return _fieldTypeName;
    }

    protected String getColName() {
      return _colName;
    }

    protected int getPrecision() {
      return _precision;
    }

    protected int getScale() {
      return _scale;
    }
  }

  protected static class TableMetadata {
    private String _columnSchemaName;
    private String _columnFieldTypeName;
    private String _colName;
    private int _precision;
    private int _scale;

    protected TableMetadata(@NotNull String colTypeName, @NotNull String colName, int precision, int scale) {
      String[] columnTypeParts = colTypeName.split("\\.");

      if (columnTypeParts.length == 1) {
        _columnSchemaName = null;
        _columnFieldTypeName = columnTypeParts[0];
      } else {
        _columnSchemaName = columnTypeParts[0];
        _columnFieldTypeName = columnTypeParts[1];
      }

      _precision = precision;
      _scale = scale;
      _colName = colName;
    }

    protected String getColumnSchemaName() {
      return _columnSchemaName;
    }

    protected String getColumnFieldTypeName() {
      return _columnFieldTypeName;
    }

    protected String getColName() {
      return _colName;
    }

    protected int getPrecision() {
      return _precision;
    }

    protected int getScale() {
      return _scale;
    }
  }
}