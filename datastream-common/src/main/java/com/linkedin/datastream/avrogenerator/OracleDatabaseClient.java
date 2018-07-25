package com.linkedin.datastream.avrogenerator;

import java.sql.ResultSetMetaData;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.stream.Collectors;
import java.util.List;
import java.sql.PreparedStatement;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.ResultSet;
import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.datastream.common.DynamicDataSourceFactoryImpl;




/**
 * The OracleDatabaseClient handles all of the communication to the Oracle Database
 */
public class OracleDatabaseClient extends DatabaseSource {
  private static final Logger LOG = LoggerFactory.getLogger(OracleDatabaseClient.class);

  private static final String PK_ALIAS = "primary_key";
  private static final String COLLECTION_INFO_TABLE = "ALL_COLL_TYPES";
  private static final String STRUCT_INFO_TABLE = "ALL_TYPE_ATTRS";

  private static final int TABLE_NAME_INDEX = 2;
  private static final int TABLE_NAME_INDEX_PK = 1;

  private static final int SCHEMA_NAME_INDEX = 1;
  private static final int FIELD_TYPE_NAME_INDEX = 2;
  private static final int PRECISION_INDEX = 3;
  private static final int SCALE_INDEX = 4;
  private static final int COL_NAME_INDEX = 5;

  private static final List<String> PRIMITIVE_TYPES =
      Arrays.asList(Types.values()).stream().map(t -> t.name()).collect(Collectors.toList());

  private DataSource _dataSource;
  private Connection _conn;
  private final String _connectionUri;

  public OracleDatabaseClient(String connectionUri) {
    _connectionUri = connectionUri;
  }

  public void initializeConnection() throws SQLException {
    if (_dataSource != null && _conn != null) {
      return;
    }

    try {
      DynamicDataSourceFactoryImpl dataSourceFactory = new DynamicDataSourceFactoryImpl();
      _dataSource = dataSourceFactory.createOracleDataSource(_connectionUri, -1);
      _conn = _dataSource.getConnection();
    } catch (Exception e) {
      throw new SQLException(e);
    }
  }

  public void closeConnection() {
    if (_conn == null) {
      return;
    }

    try {
      _conn.close();
      _conn = null;
    } catch (SQLException e) {
      LOG.warn(e.getMessage());
    }
  }

  public List<String> getPrimaryKeyFields(String tableName) throws SQLException, SchemaGenerationException {
    String sql = primaryKeySqlQuery();

    ResultSet rs = null;
    PreparedStatement stmt = null;
    ArrayList<String> pkList = new ArrayList<>();

    try {
      stmt = _conn.prepareStatement(sql);
      stmt.setString(TABLE_NAME_INDEX_PK, tableName);
      rs = stmt.executeQuery();

      while (rs.next()) {
        pkList.add(rs.getString(PK_ALIAS));
      }
    } finally {
      releaseResources(rs, stmt);
    }

    if (pkList.size() > 1) {
      throw new SchemaGenerationException(
          String.format("source: %s uses CompositeKeys: %s. This OracleDatabaseClient does not support CompositeKeys",
              tableName, pkList.toString()));
    }

    return pkList;
  }

  public List<String> getAllFields(String tableName, String schemaName) throws SQLException {
    String sql = fieldsSqlQuery();

    ResultSet rs = null;
    PreparedStatement stmt = null;
    ArrayList<String> colNames = new ArrayList<>();

    try {
      stmt = _conn.prepareStatement(sql);
      stmt.setString(SCHEMA_NAME_INDEX, schemaName);
      stmt.setString(TABLE_NAME_INDEX, tableName);

      rs = stmt.executeQuery();

      while (rs.next()) {
        colNames.add(rs.getString("column_name"));
      }

    } finally {
      releaseResources(rs, stmt);
    }

    return colNames;
  }

  public boolean isTable(String schemaName, String tableName) {
    String fullTableName = schemaName + "." + tableName;
    String sql = tableInfoSql(fullTableName);
    PreparedStatement stmt = null;

    try {
      stmt = _conn.prepareStatement(sql);
      stmt.executeQuery();
      return true;
    } catch (SQLException e) {
      LOG.error(String.format("Failed to determine isTable() for schema %s, table %s", schemaName, tableName), e);
      return false;
    } finally {
      releaseResources(null, stmt);
    }
  }

  /**
   * Determine if the fieldTypeName passed in the argument is one of the built in
   * Oracle Types. This class maintains a {@code PRIMITIVE_TYPES} which stores the
   * names of all the pre built Types.
   */
  public boolean isPrimitive(String fieldTypeName) throws SQLException {
    return PRIMITIVE_TYPES.contains(fieldTypeName);
  }

  /**
   * Run a query against the metadata tables in Oracle to determine if fieldType in
   * the argument is a type of Struct
   */
  public boolean isStruct(String schemaName, String fieldTypeName) throws SQLException {
    String sql = classifyTypeSqlQuery(STRUCT_INFO_TABLE);
    ResultSet rs = null;
    PreparedStatement stmt = null;

    try {
      stmt = _conn.prepareStatement(sql);
      stmt.setString(SCHEMA_NAME_INDEX, schemaName);
      stmt.setString(FIELD_TYPE_NAME_INDEX, fieldTypeName);

      rs = stmt.executeQuery();

      // If Struct information exists then it is a Struct type
      return rs.next();
    } finally {
      releaseResources(rs, stmt);
    }
  }

  /**
   * Run a query against the metadata tables in Oracle to determine if fieldType in
   * the argument is a type of Collection
   */
  public boolean isCollection(String schemaName, String fieldTypeName) throws SQLException {
    String sql = classifyTypeSqlQuery(COLLECTION_INFO_TABLE);
    ResultSet rs = null;
    PreparedStatement stmt = null;

    try {
      stmt = _conn.prepareStatement(sql);
      stmt.setString(SCHEMA_NAME_INDEX, schemaName);
      stmt.setString(FIELD_TYPE_NAME_INDEX, fieldTypeName);

      rs = stmt.executeQuery();

      // If collection information exists then it is a collection type
      return rs.next();
    } finally {
      releaseResources(rs, stmt);
    }
  }

  public List<TableMetadata> getTableMetadata(String schemaName, String tableName) throws SQLException {
    String fullTableName = schemaName + "." + tableName;
    String sql = tableInfoSql(fullTableName);
    ResultSet rs = null;
    PreparedStatement stmt = null;

    List<TableMetadata> tableMetadataList = new ArrayList<>();

    try {
      stmt = _conn.prepareStatement(sql);
      rs = stmt.executeQuery();
      ResultSetMetaData rsmd = rs.getMetaData();
      int colCount = rsmd.getColumnCount();

      for (int i = 1; i <= colCount; i++) {
        String nullable = "";
        int nullableMetadata = rsmd.isNullable(i);
        if (nullableMetadata == ResultSetMetaData.columnNullable) {
          nullable = "Y";
        } else if (nullableMetadata == ResultSetMetaData.columnNoNulls) {
          nullable = "N";
        }
        tableMetadataList.add(new TableMetadata(rsmd.getColumnTypeName(i),
            rsmd.getColumnName(i),
            nullable,
            rsmd.getPrecision(i),
            rsmd.getScale(i)));
      }

      return tableMetadataList;
    } finally {
      releaseResources(rs, stmt);
    }
  }

  public List<StructMetadata> getStructMetadata(String schemaName, String fieldTypeName) throws SQLException {
    String sql = typeInfoSqlQuery();
    ResultSet rs = null;
    PreparedStatement stmt = null;

    List<StructMetadata> metadataList = new ArrayList<>();

    try {
      stmt = _conn.prepareStatement(sql);
      stmt.setString(SCHEMA_NAME_INDEX, schemaName);
      stmt.setString(FIELD_TYPE_NAME_INDEX, fieldTypeName);

      rs = stmt.executeQuery();

      while (rs.next()) {
        metadataList.add(new StructMetadata(rs.getString(SCHEMA_NAME_INDEX),
            rs.getString(FIELD_TYPE_NAME_INDEX),
            rs.getString(COL_NAME_INDEX),
            rs.getInt(PRECISION_INDEX),
            rs.getInt(SCALE_INDEX)));
      }

      return metadataList;

    } finally {
      releaseResources(rs, stmt);
    }
  }

  public CollectionMetadata getCollectionMetadata(String schemaName, String fieldTypeName) throws SQLException {
    String sql = collectionTypeInfoSql();
    ResultSet rs = null;
    PreparedStatement stmt = null;

    try {
      stmt = _conn.prepareStatement(sql);
      stmt.setString(SCHEMA_NAME_INDEX, schemaName);
      stmt.setString(FIELD_TYPE_NAME_INDEX, fieldTypeName);

      rs = stmt.executeQuery();

      rs.next();

      return new CollectionMetadata(rs.getString(SCHEMA_NAME_INDEX),
          rs.getString(FIELD_TYPE_NAME_INDEX),
          rs.getInt(PRECISION_INDEX),
          rs.getInt(SCALE_INDEX));

    } finally {
      releaseResources(rs, stmt);
    }
  }

  /**
   * Generate the SQL query to grab the primary keys from a specific table/view
   * from an Oracle Database
   *
   * SELECT
   *  cols.column_name as primary_key
   * FROM
   *  all_constraints cons,
   *  all_cons_columns cols
   * WHERE
   *  cols.table_name = <tableName> AND
   *  cons.constraint_type = 'P' AND
   *  cons.constraint_name = cols.constraint_name AND
   *  cons.owner = cols.owner
   * ORDER BY
   *  cols.position
   *
   */
  private static String primaryKeySqlQuery() {
    StringBuilder pkQuery = new StringBuilder();

    pkQuery.append("SELECT cols.column_name as ").append(PK_ALIAS).append(" ");
    pkQuery.append("FROM all_constraints cons, all_cons_columns cols ");
    pkQuery.append("WHERE cols.table_name = ? ")
        .append("AND cons.constraint_type = 'P' ")
        .append("AND cons.constraint_name = cols.constraint_name ")
        .append("AND cons.owner = cols.owner ");
    pkQuery.append("ORDER BY cols.position");

    return pkQuery.toString();
  }

  /**
   * SELECT
   *  1
   * FROM
   *  <metadataTable>
   * WHERE
   *  owner = <schemaName> AND
   *  type_name = <sqlType> AND
   *  rownum < 2
   *
   * @param metadataTable (all_coll_types / all_types_attr) will determine which type we are
   *                      trying to classify, (Structs or Collections)
   */
  private static String classifyTypeSqlQuery(String metadataTable) {
    return "SELECT 1 FROM " + metadataTable + " WHERE owner=? AND type_name=? and rownum < 2";
  }

  /**
   * Generate the SQL query to grab all the field name from a specific table/view
   * from an Oracle Database
   *
   * SELECT
   *   column_name
   * FROM
   *   all_tab_cols
   * WHERE
   *   table_name = <tableName> AND
   *   owner = <dBName>
   *
   */
  private static String fieldsSqlQuery() {
    return "SELECT column_name FROM all_tab_columns WHERE owner=? and table_name=?";
  }

  private static String typeInfoSqlQuery() {
    StringBuilder typeQuery = new StringBuilder();

    typeQuery.append("SELECT ATTR_TYPE_OWNER, ATTR_TYPE_NAME, PRECISION, SCALE, ATTR_NAME ");
    typeQuery.append("FROM ALL_TYPE_ATTRS ");
    typeQuery.append("WHERE OWNER=? ")
        .append("AND TYPE_NAME=? ");
    typeQuery.append("ORDER BY ATTR_NO");

    return typeQuery.toString();
  }

  private static String collectionTypeInfoSql() {
    StringBuilder sqlQuery = new StringBuilder();

    sqlQuery.append("SELECT ELEM_TYPE_OWNER, ELEM_TYPE_NAME, PRECISION, SCALE ");
    sqlQuery.append("FROM ALL_COLL_TYPES ");
    sqlQuery.append("WHERE OWNER=? ")
        .append("AND TYPE_NAME=?");

    return sqlQuery.toString();
  }

  private static String tableInfoSql(String fullTableName) {
    return "SELECT * FROM " + fullTableName + " WHERE 0=1";
  }

  /**
   * Close the ResultSet and the PreparedStatement for better resource management.
   */
  private static void releaseResources(ResultSet rs, PreparedStatement stmt) {
    if (rs != null) {
      try {
        rs.close();
      } catch (SQLException e) {
        LOG.warn("Failed to close ResultSet " + stmt, e);
      }
    }

    if (stmt != null) {
      try {
        stmt.close();
      } catch (SQLException e) {
        LOG.warn("Failed to close PreparedStatement " + stmt, e);
      }
    }
  }

}
