package com.linkedin.datastream.connectors.mysql;

import java.net.URI;
import java.net.URISyntaxException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.datastream.connectors.mysql.MysqlConnector.SourceType;
import com.linkedin.jersey.api.uri.UriBuilder;
import com.linkedin.r2.util.URIUtil;


/**
 * The source of a mysql datastream. It provides information of db name,
 * table name, host name, and port number.
 *
 * The valid format of the source string:
 * mysql://[HostName]:[Port]/[DBName]/[TableName]
 */
public class MysqlSource {
  private static final Logger LOG = LoggerFactory.getLogger(MysqlSource.class);
  public static final String MYSQL_SCHEMA = "mysql";
  public static final String ALL_TABLES = "all";

  private String _binlogFolderName = "";
  private String _databaseName;
  private String _tableName;
  private String _hostName;
  private int _port;
  private String _connectionStr;

  private SourceType _sourceType;

  public MysqlSource(String hostName, int port, String databaseName, String tableName) {
    _sourceType = SourceType.MYSQLSERVER;
    _hostName = hostName;
    _port = port;
    _databaseName = databaseName;
    _tableName = tableName;
    UriBuilder builder = new UriBuilder();
    builder.scheme(MYSQL_SCHEMA).host(hostName).port(port).path(String.format("/%s/%s", databaseName, tableName));
    _connectionStr = builder.build().toASCIIString();
  }

  public MysqlSource(String binlogFolderName, String databaseName, String tableName) {
    _sourceType = SourceType.MYSQLBINLOG;
    _hostName = _sourceType.toString();
    _port = 0;
    _databaseName = databaseName;
    _tableName = tableName;
    _binlogFolderName = binlogFolderName;
    UriBuilder builder = new UriBuilder();
    builder.scheme(MYSQL_SCHEMA)
        .host(_hostName)
        .port(_port)
        .path(String.format("/%s/%s", databaseName, tableName))
        .queryParam("folder", binlogFolderName);

    _connectionStr = builder.build().toASCIIString();
  }

  /**
   * Parses the uri for Mysql connector's source.
   * @param uri
   *   Uri that needs to be parsed. Uri should be of the format mysql://hostName:port/DatabaseName/TableName
   *   TableName is optional, if you want to capture events from all the tables.
   * @return
   */
  public static MysqlSource createFromUri(String uri) throws SourceNotValidException {
    URI sourceUri;
    try {
      sourceUri = new URI(uri);
    } catch (URISyntaxException e) {
      String msg = String.format("Unable to parse the Uri %s", uri);
      LOG.error(msg, e);
      throw new SourceNotValidException(msg, e);
    }

    String hostName = sourceUri.getHost();
    int port = sourceUri.getPort();
    String[] pathFragments = URIUtil.tokenizePath(sourceUri.getPath());
    if (pathFragments.length != 1 && pathFragments.length != 2) {
      throw new SourceNotValidException("Uri should be of the format mysql://hostName:port/DatabaseName/TableName");
    }

    if (hostName.equalsIgnoreCase(SourceType.MYSQLBINLOG.toString())) {
      String binlogFolderName = sourceUri.getQuery().split("=")[1];
      return new MysqlSource(binlogFolderName, pathFragments[0], pathFragments[1]);
    } else {
      return new MysqlSource(hostName, port, pathFragments[0], pathFragments[1]);
    }
  }

  public SourceType getSourceType() {
    return _sourceType;
  }

  public String getBinlogFolderName() {
    return _binlogFolderName;
  }

  public String getHostName() {
    return _hostName;
  }

  public int getPort() {
    return _port;
  }

  public String getDatabaseName() {
    return _databaseName;
  }

  public String getTableName() {
    return _tableName;
  }

  public boolean isAllTables() {
    return _tableName.equalsIgnoreCase(ALL_TABLES);
  }

  @Override
  public String toString() {
    return _connectionStr;
  }
}
