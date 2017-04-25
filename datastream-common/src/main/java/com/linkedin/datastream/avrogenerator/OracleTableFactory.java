package com.linkedin.datastream.avrogenerator;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import com.google.common.base.CaseFormat;
import com.google.common.base.Joiner;


/**
 * {@link FieldType} classes are used as wrappers around database Types. This
 * Factory class is responsible for creating these classes as needed
 *
 * This class does this by running special queries against the metadata tables in
 * in the Oracle database in order to grab information such as Oracle types, Column Names,
 * Precision and Scale of Numerics etc.
 */
public class OracleTableFactory {
  private DatabaseSource _databaseSource;

  public OracleTableFactory(DatabaseSource databaseSource) {
    _databaseSource = databaseSource;
  }

  /**
   * Build an OracleTable FieldType. OracleTable FieldTypes are a wrapper for top level
   * Database Tables, this is very similar to the OracleStructType with the exception that
   * there is a primaryKey.
   *
   * Column information is gotten through {@code _databaseSource}, we then recursively call
   * {@code #buildFieldType} in order to build each child Column.
   *
   * @param schemaName The SchemaName, associated with the Owner of the table
   * @param tableName The table name
   * @param primaryKey The primary key that we want to use
   * @return an OracleTable instance which represents the entire table
   * @throws SQLException
   */
  public OracleTable buildOracleTable(String schemaName, String tableName, String primaryKey) throws SQLException, SchemaGenerationException {
    List<OracleDatabaseClient.TableMetadata> metadataList = _databaseSource.getTableMetadata(schemaName, tableName);

    List<OracleColumn> childColumns = new ArrayList<>();

    for (OracleDatabaseClient.TableMetadata metadata : metadataList) {
      String colName = metadata.colName();

      FieldType childFieldType = buildFieldType(metadata.columnSchemaName(),
          metadata.columnFieldTypeName(),
          metadata.precision(),
          metadata.scale());

      childColumns.add(new OracleColumn(colName, childFieldType, childColumns.size()));
    }

    if (primaryKey == null) {
      List<String> primaryKeys = _databaseSource.getPrimaryKeyFields(tableName);

      if (primaryKeys.size() == 0) {
        throw new SchemaGenerationException(String.format("Failed to retrieve primary keys from source: %s. Please call #setPrimaryKey()",
            tableName));
      }

      // build primaryKey string as comma-separated list of the primary key(s)
      primaryKey = Joiner.on(",")
          .join(primaryKeys.stream()
              .map(pk -> CaseFormat.UPPER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, pk))
              .collect(Collectors.toList()));
    }

    return new OracleTable(tableName, schemaName, childColumns, primaryKey);
  }

  /**
   * Recursively build a FieldType that properly wraps around a Database Type.
   *
   * If the arguments to this function refer to a NUMBER, they should also include
   * precision and scale to classify the type into (long, float, double, int)
   *
   * @param schemaName some Database Types (Structs and Collections) have their own schemaName
   * @param fieldTypeName name of the Database Type (CHAR, VARCHAR, CUSTOM_STRUCT,.. etc)
   * @param precision precision of Numeric Types
   * @param scale scale of Numeric Types
   */
  private FieldType buildFieldType(String schemaName, String fieldTypeName, int precision, int scale)
      throws SQLException {

    if (_databaseSource.isPrimitive(fieldTypeName)) {
      return buildOraclePrimitive(fieldTypeName, precision, scale);
    }

    if (_databaseSource.isCollection(schemaName, fieldTypeName)) {
      return buildOracleCollection(schemaName, fieldTypeName);
    }

    if (_databaseSource.isStruct(schemaName, fieldTypeName)) {
      return buildOracleStruct(schemaName, fieldTypeName);
    }

    throw new SQLException(String.format("Cannot determine type info for the attribute (%s.%s)", schemaName, fieldTypeName));
  }

  /**
   * Build an OraclePrimitiveType FieldType. These include types such as (VARCHAR2, CHAR, etc)
   *
   * @param fieldTypeName name of the FieldType, (CHAR, VARCHAR)
   * @param precision the precision of the NUMERIC (to help determine LONG vs INT)
   * @param scale the scale of the numeric (to help determine DOUBLE vs FLOAT)
   */
  private FieldType buildOraclePrimitive(String fieldTypeName, int precision, int scale) {
    return new OraclePrimitiveType(fieldTypeName, scale, precision);
  }

  /**
   * Build an OracleCollectionType FieldType. These are ARRAY types. Each OracleCollectionType instance of course
   * is composed of element FieldTypes. These element FieldTypes are recursively built by calling
   * {@code #buildFieldType} using the metadata of the parent OracleCollectionType
   *
   * @param schemaName
   * @param fieldTypeName
   * @return
   * @throws SQLException
   */
  private FieldType buildOracleCollection(String schemaName, String fieldTypeName) throws SQLException {
    OracleDatabaseClient.CollectionMetadata metadata = _databaseSource.getCollectionMetadata(schemaName, fieldTypeName);

    FieldType elementFieldType = buildFieldType(metadata.elementSchemaName(),
        metadata.elementFieldTypeName(),
        metadata.elementPrecision(),
        metadata.elementScale());

    return new OracleCollectionType(schemaName, fieldTypeName, elementFieldType);
  }

  /**
   * Build an OracleStructType FieldType. These are Struct types, each OracleStructType is essentially its own
   * database table. Each OracleStructType is composed of {@code OracleColumn} where each {@code OracleColumn}
   * has its own name and FieldType. An OracleStructType is built by recursively calling {@code #buidlFieldType}
   * on each of the child columns.
   *
   * @param schemaName
   * @param fieldTypeName
   * @return
   * @throws SQLException
   */
  private FieldType buildOracleStruct(String schemaName, String fieldTypeName) throws SQLException {
    List<OracleDatabaseClient.StructMetadata> metadataList = _databaseSource.getStructMetadata(schemaName, fieldTypeName);
    List<OracleColumn> childColumns = new ArrayList<>();

    for (OracleDatabaseClient.StructMetadata metadata : metadataList) {
      FieldType childFieldType = buildFieldType(metadata.schemaName(),
          metadata.fieldTypeName(),
          metadata.precision(),
          metadata.scale());

      childColumns.add(new OracleColumn(metadata.colName(), childFieldType, childColumns.size()));
    }

    return new OracleStructType(schemaName, fieldTypeName, childColumns);
  }
}
