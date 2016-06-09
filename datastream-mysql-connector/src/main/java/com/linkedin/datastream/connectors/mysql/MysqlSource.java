package com.linkedin.datastream.connectors.mysql;

import java.net.URI;

import com.linkedin.r2.util.URIUtil;
import java.net.URISyntaxException;


/**
 * The source of a mysql datastream. It provides information of db name,
 * table name, host name, and port number.
 *
 * The valid format of the source string:
 * mysql://[HostName]:[Port]/[DBName]/[TableName]
 */
public class MysqlSource {
  public static final String MYSQL_SCHEMA = "mysql";

  private String _databaseName;
  private String _tableName;
  private String _hostName;
  private int _port;
  private String _connectionStr;

  public MysqlSource(String hostName, int port, String databaseName, String tableName) {
    _hostName = hostName;
    _port = port;
    _databaseName = databaseName;
    _tableName = tableName;
    try {
      URI sourceUri = new URI(MYSQL_SCHEMA, null, hostName, port, String.format("/%s/%s", databaseName, tableName), null, null);
      _connectionStr = sourceUri.toString();
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Parses the uri for Mysql connector's source.
   * @param uri
   *   Uri that needs to be parsed. Uri should be of the format mysql://hostName:port/DatabaseName/TableName
   *   TableName is optional, if you want to capture events from all the tables.
   * @return
   */
  public static MysqlSource createFromUri(String uri)
      throws SourceNotValidException {
    URI sourceUri = URI.create(uri);
    String hostName = sourceUri.getHost();
    int port = sourceUri.getPort();
    String[] pathFragments = URIUtil.tokenizePath(sourceUri.getPath());
    if (pathFragments.length != 1 && pathFragments.length != 2) {
      throw new SourceNotValidException("Uri should be of the format mysql://hostName:port/DatabaseName/TableName");
    }

    return new MysqlSource(hostName, port, pathFragments[0], pathFragments[1]);
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

  @Override
  public String toString() {
    return _connectionStr;
  }
}
