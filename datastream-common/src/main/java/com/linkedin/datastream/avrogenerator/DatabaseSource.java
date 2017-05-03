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
  void initializeConnection() throws SQLException {
    // default implementation is no-op
  }

  /**
   * Close connection to the database
   */
  void closeConnection() {
    // default implementation is no-op
  }

  /**
   * Determine if the arguments points to a table or view in the Database
   * @return true is exists, false otherwise
   */
  abstract boolean isTable(String schemaName, String tableName);

  /**
   * Determine if the arguments point to a Collection type in the table
   * @return true is collection, false otherwise
   */
  abstract boolean isCollection(String schemaName, String fieldTypeName) throws SQLException;

  /**
   * Determine if the arguments point to Struct Type in the table
   * @return true if Struct, false otherwise
   */
  abstract boolean isStruct(String schemaName, String fieldTypeName) throws SQLException;

  /**
   * Determine if the fieldTypeName is one of the primitive types of the Database
   * @return true if primitive
   */
  abstract boolean isPrimitive(String fieldTypeName) throws SQLException;

  /**
   * Retrieve the table Metadata containing information such as all of Column names
   * and their types.
   */
  abstract List<TableMetadata> getTableMetadata(String schemaName, String tableName) throws SQLException;

  /**
   * Retrieve the metadata of the Struct type. Contains information about the child columns that are
   * associated with this Struct Type
   */
  abstract List<StructMetadata> getStructMetadata(String schemaName, String fieldTypeName) throws SQLException;

  /**
   * Retrieve the metadata of the Collection Type. Contains information about the elements of the
   * Array Type
   */
  abstract CollectionMetadata getCollectionMetadata(String schemaName, String fieldTypeName) throws SQLException;

  /**
   * Create and run a query to get all primary keys from the Database table/view
   */
  abstract List<String> getPrimaryKeyFields(String tableName) throws SQLException, SchemaGenerationException;

  /**
   * Create and execute a query to get all the fields from the Database table/view
   */
  abstract List<String> getAllFields(String tableName, String dbName) throws SQLException;


  protected static class CollectionMetadata {
    private String _fieldName;
    private String _schemaName;
    private int _precision;
    private int _scale;

    public CollectionMetadata(@NotNull String schemaName, @NotNull  String fieldName, int precision, int scale) {
      _fieldName = fieldName;
      _schemaName = schemaName;
      _precision = precision;
      _scale = scale;
    }

    String elementFieldTypeName() {
      return _fieldName;
    }

    String elementSchemaName() {
      return _schemaName;
    }

    int elementPrecision() {
      return _precision;
    }

    int elementScale() {
      return _scale;
    }
  }


  protected static class StructMetadata {
    private String _fieldTypeName;
    private String _schemaName;
    private String _colName;
    private int _precision;
    private int _scale;


    StructMetadata(String schemaName, @NotNull String fieldTypeName, @NotNull String colName, int precision, int scale) {
      _fieldTypeName = fieldTypeName;
      _schemaName = schemaName;
      _colName = colName;
      _precision = precision;
      _scale = scale;
    }

    String schemaName() {
      return _schemaName;
    }

    String fieldTypeName() {
      return _fieldTypeName;
    }

    String colName() {
      return _colName;
    }

    int precision() {
      return _precision;
    }

    int scale() {
      return _scale;
    }
  }

  protected static class TableMetadata {
    private String _columnSchemaName;
    private String _columnFieldTypeName;
    private String _colName;
    private int _precision;
    private int _scale;

    TableMetadata(@NotNull String colTypeName, @NotNull String colName, int precision, int scale) {
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

    String columnSchemaName() {
      return _columnSchemaName;
    }

    String columnFieldTypeName() {
      return _columnFieldTypeName;
    }

    String colName() {
      return _colName;
    }

    int precision() {
      return _precision;
    }

    int scale() {
      return _scale;
    }
  }
}