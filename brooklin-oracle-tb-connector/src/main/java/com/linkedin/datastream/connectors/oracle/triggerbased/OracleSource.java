package com.linkedin.datastream.connectors.oracle.triggerbased;

import java.net.URI;
import java.net.URISyntaxException;

import com.linkedin.datastream.common.DatastreamRuntimeException;


public class OracleSource {
  private static final String ORACLE_SCHEME = "oracle";
  private static final String CONNECTION_FORMAT = "oracle:/[DbName]/[ViewName]";

  private final String _viewName;
  private final String _dbName;
  private String _connectionString;

  public OracleSource(String connectionString) {
    if (connectionString == null) {
      throw new DatastreamRuntimeException("connectionString not supplied");
    }

    _connectionString = connectionString.replaceAll("\\s+", "");

    try {
      URI uri = new URI(_connectionString);

      // validate the connection string exists
      if (uri.getScheme() == null || !uri.getScheme().equalsIgnoreCase(ORACLE_SCHEME)) {
        throw new DatastreamRuntimeException(String.format("Invalid schema in the uri. Expect %s. Actual: %s",
            ORACLE_SCHEME,
            uri.getScheme()));
      }

      String[] elements = uri.getPath().substring(1).split("/");

      // validate correct arguments
      if (elements.length != 2) {
        throw new IllegalArgumentException(String.format("Invalid path in the uri Expect %s. Actual: %s",
            CONNECTION_FORMAT,
            uri.getPath()));
      }

      _dbName = elements[0];
      _viewName = elements[1];

    } catch (URISyntaxException e) {
      throw new DatastreamRuntimeException("Failed to parse Oracle source", e);
    }
  }

  public OracleSource(String viewName, String dbName) {
    _viewName = viewName;
    _dbName = dbName;
  }

  public String getViewName() {
    return _viewName;
  }
  public String getDbName() {
    return _dbName;
  }
  public String getConnectionString() {
    return _connectionString;
  }
}
