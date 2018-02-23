package com.linkedin.datastream.connectors.oracle.triggerbased.consumer;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLTimeoutException;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import javax.sql.DataSource;

import org.apache.avro.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.datastream.common.DatastreamException;
import com.linkedin.datastream.common.ErrorLogger;
import com.linkedin.datastream.common.ReflectionUtils;
import com.linkedin.datastream.common.databases.OracleDataSourceFactory;
import com.linkedin.datastream.connectors.oracle.triggerbased.OracleSource;


/**
 * The OracleConsumer Class is responsible for 3 things
 *
 * 1. generate SQL Strings
 * 2. execute those queries on the OracleDatabase
 * 3. Convert ResultSet into the OracleChangeEvents through the OracleTbTableReader class
 *
 * The OracleTaskHandler class should only interface with this class
 * and the underlying handling of the Oracle Database should be hidden
 *
 * Query Logic:
 *
 * TxLog is a commit log in the form of a table that keeps track
 * of which rows from which tables have been changed and when. The
 * major benefit of using the TxLog is that it allows us to observe
 * the commit log of transactions that span across multiple tables.
 *
 * The TxLog table is maintained by the Oracle Database Administrator
 * and operates through the use of Triggers. Triggers have been placed
 * on targeted tables in order keep an up to date TxLog table when ever
 * a change gets committed.
 */
public class OracleConsumer {
  private static final Logger LOG = LoggerFactory.getLogger(OracleConsumer.class);

  private static final Long MAX_SCN = Long.MAX_VALUE;
  private static final String TX_LOG_NAME = "sy$txlog";
  private static final String TX_LOG_ALIAS = "tx";
  private static final String SOURCE_ALIAS = "src";
  private Schema _schema;

  private final OracleSource _source;

  private DataSource _dataSource;
  private Connection _conn;
  private final String _uri;
  private final String _queryHint;
  private final int _queryTimeout;
  private final String _dataSourceFactoryClass;
  private final int _fetchSize;
  private final String _changeCaptureQuery;
  private PreparedStatement _changeCaptureStatement;


  public OracleConsumer(OracleConsumerConfig config, OracleSource source, Schema schema) throws DatastreamException {
    _source = source;

    _uri = config.getDbUri();
    _queryHint = config.getQueryHint();
    _dataSourceFactoryClass = config.getDataSourceFactoryClass();
    _queryTimeout = config.getQueryTimeout();
    _fetchSize = config.getFetchSize();
    _schema = schema;

    _changeCaptureQuery = generateEventQuery(_source, _queryHint);
  }

  /**
   * Start a connection to the Oracle Database
   */
  public void initializeConnection() throws DatastreamException {
    if (_dataSource != null && _conn != null) {
      return;
    }

    // dynamically instantiate an Oracle DataSource factory
    OracleDataSourceFactory dataSourceFactory = ReflectionUtils.createInstance(_dataSourceFactoryClass);
    if (dataSourceFactory == null) {
      String msg = "Invalid class name for OracleDataSourceFactory, class=" + _dataSourceFactoryClass;
      ErrorLogger.logAndThrowDatastreamRuntimeException(LOG, msg, null);
    }

    try {
      _dataSource = dataSourceFactory.createOracleDataSource(_uri, _queryTimeout);
      resetConnection();
    } catch (Exception e) {
      LOG.error(String.format("Failed to create an Oracle Data Source for OracleConsumer: %s", getName()));
      throw new DatastreamException(e);
    }

  }

  /**
   * Close the connection to the Oracle Database
   */
  public void closeConnection() {
    if (_conn == null) {
      return;
    }

    try {
      _conn.close();
      _conn = null;
      LOG.info(String.format("Closing OracleConsumer: %s", getName()));
    } catch (SQLException e) {
      LOG.warn(String.format("OracleConsumer: %s Failed to close connection", getName()), e);
    }
  }

  /**
   * Since there is one Oracle Consumer per DatastreamTask, We can use the
   * source connection string to uniquely identify the OracleConsumer
   */
  public String getName() {
    return _source.getConnectionString();
  }

  /**
   * Given the {@code lastScn}, determine all changed rows from the {@code _source} that occurred
   * after {@code lastScn}. The result of that query will then be converted into a list of
   * OracleChangeEvents.
   *
   * @param lastScn only look for changes that have a higher scn then lastScn
   * @return
   * @throws SQLException
   */
  public List<OracleChangeEvent> consume(long lastScn) throws SQLException {
    // validate connection has been established
    if (_conn == null) {
      String msg = String.format("OracleConsumer: %s Does not have a connection", getName());
      LOG.error(msg);
      throw new SQLException(msg);
    }

    // Prepared statements improve performance by having the database compile and build the
    // query plan for the statement once. We dont want release this resource or create a new
    // one every time since the only thing that changes are the parameters of the where
    // clause
    if (_changeCaptureStatement == null) {
      _changeCaptureStatement = generatePreparedStatement(_changeCaptureQuery, -1);
      _changeCaptureStatement.setFetchSize(_fetchSize);
    }


    // execute the prepared statement
    ResultSet rs = null;
    try {
      // bind the {@code lastScn}, telling the query to only look at changes since {@code lastScn}
      // MAX_SCN is the largest possible scn
      _changeCaptureStatement.setLong(1, lastScn);
      _changeCaptureStatement.setLong(2, MAX_SCN);

      rs = executeQuery(_changeCaptureStatement);

      // instantiate an Oracle Table Reader to convert ResultSet to OracleChangeEvents
      OracleTbTableReader tableReader = new OracleTbTableReader(rs, _schema);

      List<OracleChangeEvent> changeEvents = new ArrayList<>();
      OracleChangeEvent changeEvent;

      // build a Change Event for each row in the ResultSet and add it to a list of Change Events
      while ((changeEvent = nextEventWithRetries(tableReader)) != null) {
        changeEvents.add(changeEvent);
      }

      return changeEvents;
    } catch (SQLException e) {
      LOG.error(String.format("Query failed: %s", _changeCaptureQuery));
      throw e;
    } finally {
      _conn.commit();
      releaseResources(rs, null);
    }
  }

  public long getLatestScn() throws SQLException {
    // validate connection has been established
    if (_conn == null) {
      String msg = String.format("OracleConsumer: %s Does not have a connection", getName());
      LOG.error(msg);
      throw new SQLException(msg);
    }

    String sql = generateMaxScnQuery();

    PreparedStatement stmt = null;
    ResultSet rs = null;
    try {
      stmt = generatePreparedStatement(sql, -1);
      rs = executeQuery(stmt);
      // There should only be one element in the resultSet
      long scn = 0;
      while (rs.next()) {
        scn = rs.getLong(1);
      }

      if (scn == 0) {
        throw new SQLException(String.format("Failed to get starting SCN for %s using sqlQuery: %s",
            getName(),
            sql));
      }

      LOG.info("latest SCN is: {}", scn);
      return scn;

    } finally {
      _conn.commit();
      releaseResources(rs, stmt);
    }
  }

  /**
   * Builds the sql EventQuery to grab changes from Oracle
   *
   * select
   *  tx.scn scn,
   *  tx.ts event_timestamp,
   *  src.*
   * from
   *  sy$testView src,
   *  sy$txlog tx
   * where
   *  src.txn=tx.txn and
   *  tx.scn > ? and
   *  tx.scn < ?
   *
   * @param source
   * @return
   */
  public static String generateEventQuery(OracleSource source, String queryHint) {
    StringBuilder sql = new StringBuilder();
    sql.append("select ");
    sql.append(queryHint).append(" ");
    addSelectClause(source, sql);
    addFromClause(source, sql);
    addWhereClause(source, sql);

    return sql.toString();
  }


  /**
   * Builds the sql maxScn query
   *
   * select
   *  max(scn)
   * from
   *  sy$txlog
   *
   * The TxLog table represents the Commit Log for the ALL tables in the Database
   * that have been databusified. So even though this consumer only looks for changes
   * in one specific view/table, we can grab the max(scn) from the TxLog because any
   * newer SCN's for a view will also be larger than all other SCN's in the TxLog table
   */
  public static String generateMaxScnQuery() {
    return "select max(scn) from " + TX_LOG_NAME;
  }

  /**
   * Reset the connection to Oracle through {@code _dataSource}
   * We set the Transaction Isolation Level to TRANSACTION_SERIALIZABLE
   * because we want to avoid Dirty Reads and NonRepeatable Reads.
   *
   * We also set autoCommit to false because we dont want each SQL statement
   * to be committed and executed individually
   *
   * @throws SQLException
   */
  private void resetConnection() throws SQLException {
    _conn = _dataSource.getConnection();
    _conn.setAutoCommit(false);
    _conn.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
  }

  /**
   * Add the select clause the eventQuery
   *
   * select {@code scn} from the TxLog table to be used in the
   * datastreamEvent metadata
   *
   * select {@code timestamp} from the TxLog table to be used in the
   * datastreamEvent metdata
   *
   * select {@code src.*} The join predicate combined with the
   * where clause will resolve in a relation set that contains
   * all the rows that have been changed since the
   * {@code lastSCN} for the specific source {@code sy$testView}.
   *
   * select {@code src.*} will grab all of
   * those changed rows.
   *
   * @param source The source that we want to grab changes from
   * @param sqlQuery sql query
   */
  private static void addSelectClause(OracleSource source, StringBuilder sqlQuery) {
    sqlQuery.append(TX_LOG_ALIAS + ".scn scn, ");
    sqlQuery.append(TX_LOG_ALIAS + ".ts event_timestamp, ");
    sqlQuery.append(SOURCE_ALIAS + ".* ");
  }

  /**
   * add the source info to the Select clause. In the case of triggerd sources
   * we will need to a join with the TxLog table in order to first obtain
   * changed row information
   *
   * @param source The source we want to grab changes from
   * @param sqlQuery sql query
   */
  private static void addFromClause(OracleSource source, StringBuilder sqlQuery) {
    String view = source.getViewName();

    sqlQuery.append("from ");
    sqlQuery.append(view + " " + SOURCE_ALIAS + ", ");
    sqlQuery.append(TX_LOG_NAME + " " + TX_LOG_ALIAS + " ");
  }

  /**
   * Add the Where clause to the sql query. This where clause establishes the join predicate between
   * the TxLog table and the respective view through the txn column.
   *
   * @param source
   * @param sqlQuery
   */
  private static void addWhereClause(OracleSource source, StringBuilder sqlQuery) {

    sqlQuery.append("where ");
    sqlQuery.append(SOURCE_ALIAS + ".txn=" + TX_LOG_ALIAS + ".txn and ");
    sqlQuery.append(TX_LOG_ALIAS + ".scn > ? and ");
    sqlQuery.append(TX_LOG_ALIAS + ".scn < ?");
  }

  /**
   * generate a prepared Statement to be executed in the future
   *
   * @param sqlQuery the sql query to be used to generate the prepared statement
   * @param queryTimeoutSec the timeout in seconds for running the query
   * @return PreparedStatement
   * @throws SQLException
   */
  private PreparedStatement generatePreparedStatement(String sqlQuery, int queryTimeoutSec) throws SQLException {
    PreparedStatement stmt = _conn.prepareStatement(sqlQuery);

    if (queryTimeoutSec > 0) {
      stmt.setQueryTimeout(queryTimeoutSec);
    }

    return stmt;
  }

  /**
   * Execute a prepared statement
   *
   * @param stmt The Prepared Statement
   * @return the resultSet of the Query
   * @throws SQLException
   */
  private ResultSet executeQuery(PreparedStatement stmt) throws SQLException {
    try {
      return stmt.executeQuery();
    } catch (SQLTimeoutException e) {
      LOG.error("Query timed out with timeout: " + stmt.getQueryTimeout());
      throw e;
    }
  }

  /**
   * Close the ResultSet and the PreparedStatement for better resource management.
   */
  private static void releaseResources(ResultSet rs, PreparedStatement stmt) {
    if (rs != null) {
      try {
        rs.close();
      } catch (SQLException e) {
        LOG.warn("Failed to Close ResultSet", e);
      }
    }

    if (stmt != null) {
      try {
        stmt.close();
      } catch (SQLException e) {
        LOG.warn("Failed to Close PreparedStatement", e);
      }
    }
  }

  private OracleChangeEvent nextEventWithRetries(OracleTbTableReader tableReader) {
    try {
      return tableReader.next();
    } catch (SQLException e) {
      LOG.error("Error while trying to iterate through SQL");
      return null;
    } catch (NoSuchElementException | DatastreamException e) {
      return null;
    }
  }
}

