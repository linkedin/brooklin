package com.linkedin.datastream.connectors.mysql.or;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import com.linkedin.datastream.common.DatastreamException;
import com.linkedin.datastream.connectors.mysql.MysqlSource;


// Not thread-safe
public class MysqlQueryUtils {
  public static final String MODULE = MysqlQueryUtils.class.getName();
  public static final Logger LOG = Logger.getLogger(MODULE);

  // This query returns the "latest" binlog file only
  // mysql> show master status;
  //+------------------+----------+--------------+------------------+
  //    | File             | Position | Binlog_Do_DB | Binlog_Ignore_DB |
  //    +------------------+----------+--------------+------------------+
  //    | mysql-bin.000001 |      617 |              |                  |
  //+------------------+----------+--------------+------------------+

  private static final String SN_STATUS_STMT = "SHOW MASTER STATUS";
  private static final int SN_STATUS_STMT_FILENAME_POSITION = 1;
  private static final int SN_STATUS_STMT_FILEOFFSET_POSITION = 2;

  // This query returns the list of all available master logs on the server
  // mysql> show binary logs;
  // +------------------+------------+
  //    | Log_name         | File_size  |
  //    +------------------+------------+
  //    | mysql-bin.000634 | 1073741936 |
  //    | mysql-bin.000635 | 1073743032 |
  //    | mysql-bin.000636 | 1073742289 |
  //    | mysql-bin.000637 | 1073742068 |
  //    | mysql-bin.000638 | 1073742282 |
  //    | mysql-bin.000639 | 1073741981 |
  //    | mysql-bin.000640 | 1073742106 |
  //    | mysql-bin.000641 | 1073742018 |
  //    | mysql-bin.000642 | 1073742181 |
  //    | mysql-bin.000643 | 1073742017 |
  //    | mysql-bin.000644 | 1073742326 |
  //    | mysql-bin.000645 |  991499113 |
  //    +------------------+------------+
  private static final String SN_SHOW_BINARY_LOGS_STMT = "SHOW BINARY LOGS";
  private static final int SN_SHOW_BINARY_LOGS_STMT_FILENAME_POSITION = 1;

  private static final String SN_SEVER_ID_STMT = "SELECT @@server_id;";
  private static final int SN_SEVER_ID_STMT_SERVERID_POSITION = 1;

  // This is only meant to be used for unit-testing
  public static final String RESET_MASTER_STMT = "RESET MASTER;";
  public static final String FLUSH_LOGS_STMT = "FLUSH LOGS;";

  public static final String PRIMARY_COLUMN_KEY_VALUE = "PRI";

  public static final String SHOW_10_BINLOG_EVENTS_STMT = "SHOW BINLOG EVENTS LIMIT 10;";
  private static final int SHOW_BINLOG_EVENTS_LOG_NAME_POSITION = 1;
  private static final int SHOW_BINLOG_EVENTS_POS_POSITION = 2;
  private static final int SHOW_BINLOG_EVENTS_EVENT_TYPE_POSITION = 3;
  private static final int SHOW_BINLOG_EVENTS_SERVER_ID_POSITION = 4;
  private static final int SHOW_BINLOG_EVENTS_END_LOG_POS_POSITION = 5;
  private static final int SHOW_BINLOG_EVENTS_INFO_POSITION = 6;

  // timestamp, etag, flags, rstate, expires, val, schema_version
  private static final int NUM_ESPRESSO_NON_KEY_COLUMNS = 7;

  // The first 4-bytes in a mysql binlog file is a magic-header
  // This header is unlikely to change through the revisions of the mysql in the future
  // Setting this zero, causes the O/R relay to choke
  public static final int BINLOG_START_POS = 4;

  private final Map<String, TableInfo> _tableInfoMap;
  private final MysqlSource _source;
  private final String _username;
  private final String _password;

  private Connection _conn;

  // Public access methods
  public MysqlQueryUtils(MysqlSource source, String username, String password) {
    _source = source;
    _username = username;
    _password = password;
    _tableInfoMap = new HashMap<>();
  }

  public void initializeConnection() throws SQLException {
    reinit();
  }

  private void initMysqlConn() throws SQLException {
    StringBuilder urlStr = new StringBuilder();

    urlStr.append("jdbc:mysql://" + _source.getHostName());
    urlStr.append(":" + _source.getPort());

    urlStr.append("?user=").append(_username);

    if (!StringUtils.isBlank(_password)) {
      urlStr.append("&password=").append(_password);
    }

    try {
      Class.forName("com.mysql.jdbc.Driver").newInstance();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    _conn = DriverManager.getConnection(urlStr.toString());
  }

  public void closeConnection() throws SQLException {
    if (null != _conn) {
      _conn.close();
      _conn = null;
    }
  }

  /**
   * Return the earliest position ( binlog+offset) in binlog files that are available on the MASTER
   * If there are two binlog files as listed below
   *
   * mysql-bin.000634 | 1073741936
   * mysql-bin.000635 | 1073743032
   *
   * this function returns position (634,4)
   */
  public long getEarliestLogPositionOnMaster() throws SQLException {
    PreparedStatement stmt = null;
    ResultSet rs = null;
    Connection conn = getConnection();
    stmt = conn.prepareStatement(SN_SHOW_BINARY_LOGS_STMT);
    stmt.executeQuery();
    rs = stmt.getResultSet();

    if (null != rs) {
      // Obtain the first result set
      rs.next();
    } else {
      // The resultSet is either an update count or there are no more results, both of which are unexpected
      throw new RuntimeException("Error: Obtained a null resultSet for query " + SN_STATUS_STMT + " which is an error");
    }

    String file = rs.getString(SN_SHOW_BINARY_LOGS_STMT_FILENAME_POSITION);
    String[] tok = file.split("\\.");
    LOG.info(" The earliest binlog file available on master is : " + file);
    int logId = new Integer(tok[tok.length - 1]);
    int offset = BINLOG_START_POS;
    return binlogPosition(logId, offset);
  }

  /**
   * Creates a single identifier for binlog position from logId and offset
   *   SCN = {          Logid           }{         Offset         }
   *         |--- High Order 32 bits ---||--- Low Order 32 bits --|
   * @param logId
   * @param offset
   * @return
   */
  private long binlogPosition(int logId, int offset) {
    long scn = logId;
    scn <<= 32;
    scn |= offset;
    return scn;
  }

  /**
   * Simple wrapper class of a binlog filename and position.
   */
  public static class BinlogPosition {
    private final String _fileName;
    private final long _position;

    public BinlogPosition(String fileName, long position) {
      _fileName = fileName;
      _position = position;
    }

    public String getFileName() {
      return _fileName;
    }

    public long getPosition() {
      return _position;
    }
  }

  /**
   * Return the latest position ( binlog+offset) in binlog files that are available on the MASTER
   * If there are two binlog files
   *
   * mysql-bin.000634 | 1073741936
   * mysql-bin.000635 | 1073743032
   *
   * this function returns position (635,1073743032)
   */
  public BinlogPosition getLatestLogPositionOnMaster() throws SQLException {
    PreparedStatement stmt = null;
    ResultSet rs = null;
    Connection conn = getConnection();
    stmt = conn.prepareStatement(SN_STATUS_STMT);
    stmt.executeQuery();
    rs = stmt.getResultSet();

    if (null != rs) {
      // Obtain the first result set
      rs.next();
    } else {
      // The resultSet is either an update count or there are no more results, both of which are unexpected
      throw new RuntimeException("Error: Obtained a null resultSet for query " + SN_STATUS_STMT + " which is an error");
    }

    String file = rs.getString(SN_STATUS_STMT_FILENAME_POSITION);
    String[] tok = file.split("\\.");
    LOG.info(" The latest binlog file available on master is : " + file);
    int logId = new Integer(tok[tok.length - 1]);
    int offset = rs.getInt(SN_STATUS_STMT_FILEOFFSET_POSITION);
    return new BinlogPosition(file, binlogPosition(logId, offset));
  }

  /**
   * Returns the serverId of the MySQL instance that we are connected to
   */
  public int getServerId() throws SQLException, DatastreamException {
    PreparedStatement stmt = null;
    ResultSet rs = null;
    try {
      Connection conn = getConnection();
      stmt = conn.prepareStatement(SN_SEVER_ID_STMT);
      stmt.executeQuery();
      rs = stmt.getResultSet();

      if (!rs.next()) {
        throw new SQLException("ResultSet is empty");
      }
      int serverId = rs.getInt(SN_SEVER_ID_STMT_SERVERID_POSITION);
      return serverId;
    } catch (SQLException e) {
      String msg = "Error while executing query on storage node : " + _source.getHostName() + ":" + _source.getPort()
          + ", stmt = " + stmt;
      LOG.error(msg);
      throw new DatastreamException(msg, e);
    } finally {
      rs.close();
      stmt.close();
    }
  }

  /**
   * Returns the binlog events from the first available binlog file in mysql
   * WARNING : The output can be HUGE if there are lots of events. This is used only for diagnostic
   * purposes to debug test failures
   */
  public String showBinlogEvents() throws SQLException {
    PreparedStatement stmt = null;
    ResultSet rs = null;
    try {
      Connection conn = getConnection();
      stmt = conn.prepareStatement(SHOW_10_BINLOG_EVENTS_STMT);
      stmt.executeQuery();
      rs = stmt.getResultSet();

      if (!rs.next()) {
        throw new SQLException("ResultSet is empty");
      }

      String binLogEvents = rs.getString(SHOW_BINLOG_EVENTS_LOG_NAME_POSITION);
      int pos = rs.getInt(SHOW_BINLOG_EVENTS_POS_POSITION);
      String eventType = rs.getString(SHOW_BINLOG_EVENTS_EVENT_TYPE_POSITION);
      int endLogPos = rs.getInt(SHOW_BINLOG_EVENTS_END_LOG_POS_POSITION);
      String infoStr = rs.getString(SHOW_BINLOG_EVENTS_INFO_POSITION);
      String massagedOutput =
          binLogEvents + " | " + Integer.toString(pos) + " | " + eventType + " | " + endLogPos + " | " + infoStr;
      return massagedOutput;
    } finally {
      rs.close();
      stmt.close();
    }
  }

  /**
   * This is helper routine to invoke a given mysql statement
   * It is useful for cases for commands like "reset master" that do not expect a return value
   */
  public void executeQuery(String sqlStmt) throws SQLException {
    PreparedStatement stmt = null;
    ResultSet rs = null;
    try {
      Connection conn = getConnection();
      stmt = conn.prepareStatement(sqlStmt);
      stmt.executeQuery();
      stmt.close();
    } catch (Exception e) {
      LOG.error("Failed to execute statement: " + sqlStmt, e);
    } finally {
      if (rs != null) {
        rs.close();
      }
      if (stmt != null) {
        stmt.close();
      }
    }
  }

  public void executeStmt(String sqlStmt) throws SQLException {
    Statement stmt = null;
    try {
      Connection conn = getConnection();
      stmt = conn.createStatement();
      stmt.execute(sqlStmt);
      stmt.close();
    } catch (Exception e) {
      LOG.error("Failed to execute statement: " + sqlStmt, e);
    } finally {
      if (stmt != null) {
        stmt.close();
      }
    }
  }

  /**
   * Private access methods
   */
  private Connection getConnection() throws SQLException {
    int validationTimeout = 1;   // timeout in seconds
    if ((null == _conn) || _conn.isClosed() || !_conn.isValid(validationTimeout)) {
      reinit();
    }

    return _conn;
  }

  /**
   * Close existing connection if exists
   * Open a new connection to mysql
   */
  private void reinit() throws SQLException {
    if (null != _conn) {
      try {
        _conn.close();
      } catch (SQLException e) {
        LOG.warn("failed to close mysql connection to " + _source.getHostName() + ":" + _source.getPort(), e);
        // ignore it and create a new connection
      }
      _conn = null;
    }

    initMysqlConn();
  }

  public List<ColumnInfo> getColumnList(String dbName, String tableName) throws SQLException {
    // table_schema is actually espresso DB name
    String sql = "select COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, CHARACTER_OCTET_LENGTH,"
        + " NUMERIC_PRECISION, NUMERIC_SCALE FROM INFORMATION_SCHEMA.COLUMNS where table_name = '" + tableName
        + "' and table_schema= '" + dbName + "' order by ordinal_position";
    Statement stmt = null;
    ResultSet rs = null;
    try {
      LOG.info("SQL about to execute in getColumnList: " + sql);
      long timestampStart = System.currentTimeMillis();
      stmt = getConnection().createStatement();
      rs = stmt.executeQuery(sql);
      LOG.info("Time taken to execute the query: " + (System.currentTimeMillis() - timestampStart) + "ms");
      List<ColumnInfo> result = new ArrayList<ColumnInfo>();
      while (rs.next()) {
        /*
        select COLUMN_NAME,COLUMN_KEY,DATA_TYPE,CHARACTER_MAXIMUM_LENGTH,CHARACTER_OCTET_LENGTH,NUMERIC_PRECISION,NUMERIC_SCALE
        from INFORMATION_SCHEMA.COLUMNS
        where table_name='tablename_esp' and table_schema='es_dbName_0' order by ordinal_position;
        +----------------+-----------+--------------------------+------------------------+-------------------+---------------+
        | COLUMN_NAME    | DATA_TYPE | CHARACTER_MAXIMUM_LENGTH | CHARACTER_OCTET_LENGTH | NUMERIC_PRECISION | NUMERIC_SCALE |
        +----------------+-----------+--------------------------+------------------------+-------------------+---------------+
        | partitionKey   | bigint    |                     NULL |                   NULL |                19 |             0 |
        | environment    | varchar   |                      100 |                    300 |              NULL |          NULL |
        | timestamp      | bigint    |                     NULL |                   NULL |                19 |             0 |
        | etag           | varchar   |                       10 |                     30 |              NULL |          NULL |
        | flags          | bit       |                     NULL |                   NULL |                32 |          NULL |
        | rstate         | varchar   |                      256 |                    768 |              NULL |          NULL |
        | expires        | bigint    |                     NULL |                   NULL |                19 |             0 |
        | val            | longblob  |               4294967295 |             4294967295 |              NULL |          NULL |
        | schema_version | smallint  |                     NULL |                   NULL |                 5 |             0 |
          +----------------+-----------+--------------------------+------------------------+-------------------+---------------+
        */

        ColumnInfo c =
            new ColumnInfo(rs.getString(1), rs.getString(2).equals(PRIMARY_COLUMN_KEY_VALUE), rs.getString(3),
                rs.getLong(4), rs.getLong(5), rs.getLong(6), rs.getLong(7));
        result.add(c);
      }
      return result;
    } finally {
      rs.close();
      stmt.close();
    }
  }

  private List<String> getKeyColumns(String dbName, String tableName) throws SQLException {
    String sql = "SELECT k.column_name FROM information_schema.table_constraints t JOIN "
        + "information_schema.key_column_usage k USING(constraint_name,table_schema,table_name) "
        + "WHERE t.constraint_type='PRIMARY KEY' AND t.table_name = '" + tableName + "' and t.table_schema= '" + dbName
        + "' order by ordinal_position";
    Statement stmt = null;
    ResultSet rs = null;
    try {
      LOG.info("SQL about to execute in getKeyColumns: " + sql);
      long timestampStart = System.currentTimeMillis();
      stmt = getConnection().createStatement();
      rs = stmt.executeQuery(sql);
      LOG.info("Time taken to execute the query: " + (System.currentTimeMillis() - timestampStart) + "ms");
      List<String> result = new ArrayList<String>();
      while (rs.next()) {
        result.add(rs.getString(1));
      }
      return result;
    } finally {
      rs.close();
      stmt.close();
    }
  }

  /**
   * check that whether the specified db and table exist
   * @param dbName name of the database
   * @param tableName name of the table
   * @return true if the table is found under the specified db
   * @throws SQLException from execution of the sql query
   */
  public boolean checkTableExist(String dbName, String tableName) throws SQLException {
    // query the number of tables that match the db and table names
    String queryString =
        "SELECT COUNT(*) FROM information_schema.tables" + " WHERE table_schema = ? AND table_name = ?";
    PreparedStatement statement = getConnection().prepareStatement(queryString);
    statement.setString(1, dbName);
    statement.setString(2, tableName);
    ResultSet rs = statement.executeQuery();
    // return true when we get a valid and positive result
    return rs != null && rs.next() && rs.getInt(1) > 0;
  }
}
