package com.linkedin.datastream.connectors.mysql;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Properties;

import org.apache.commons.lang.Validate;

import com.linkedin.datastream.common.Datastream;
import com.linkedin.datastream.common.DatastreamDestination;
import com.linkedin.datastream.common.DatastreamSource;
import com.linkedin.datastream.connectors.mysql.or.MysqlQueryUtils;

public class MysqlConnectorTestUtils {
  private static final String TEST_HOST = "localhost";
  private static final int TEST_PORT = 3306;
  private static final String TEST_DB = "TestDB";
  private static final String TEST_TABLE = "IdNamePair";
  private static final String TEST_USER = "mysqlconnector";
  private static final String TEST_PASS = "password";
  private static final String TEST_SERVERID = "11";

  private static final String SQL_CREATE_DB = "CREATE DATABASE IF NOT EXISTS TestDB";
  private static final String SQL_SELECT_DB = "USE TestDB";
  private static final String SQL_DROP_TABLE = "DROP TABLE IF EXISTS IdNamePair";
  private static final String SQL_CREATE_TABLE = "CREATE TABLE IF NOT EXISTS IdNamePair (Id int, Name varchar(255))";
  private static final String SQL_INSERT = "INSERT IdNamePair VALUES (%d, \"%s\")";

  private static Connection getConnection(String connString) throws Exception {
    return DriverManager.getConnection(connString, "root", null);
  }

  private static MysqlQueryUtils _queryUtils;

  static {
    MysqlSource source = new MysqlSource(TEST_HOST, TEST_PORT, TEST_DB, TEST_TABLE);
    _queryUtils = new MysqlQueryUtils(source, "mysqlconnector", "password");
  }

  public static Properties getConnectorConfig() {
    Properties config = new Properties();
    config.put(MysqlConnector.CFG_MYSQL_USERNAME, TEST_USER);
    config.put(MysqlConnector.CFG_MYSQL_PASSWORD, TEST_PASS);
    config.put(MysqlConnector.CFG_MYSQL_SERVERID, TEST_SERVERID);
    return config;
  }

  public static void createTestDB() throws Exception {
    _queryUtils.executeStmt(SQL_CREATE_DB);
    _queryUtils.executeStmt(SQL_SELECT_DB);
    _queryUtils.executeStmt(SQL_DROP_TABLE);
    _queryUtils.executeStmt(SQL_CREATE_TABLE);
  }

  public static void writeMysqlEvents(int numEvents) throws Exception {
    Validate.isTrue(numEvents > 0, "number of events must be positive");
    for (int i = 0; i < numEvents; i++) {
      _queryUtils.executeStmt(String.format(SQL_INSERT, i, "Name " + i));
    }
  }

  public static Datastream createDatastream(String connString, Integer partitions) {
    Datastream stream = new Datastream();
    stream.setName("datastream_" + connString);
    stream.setConnectorType("Mysql");
    DatastreamSource source = new DatastreamSource();
    source.setConnectionString(connString);
    stream.setSource(source);
    DatastreamDestination destination = new DatastreamDestination();
    destination.setConnectionString(connString);
    destination.setPartitions(partitions);
    stream.setDestination(destination);
    return stream;
  }
}
