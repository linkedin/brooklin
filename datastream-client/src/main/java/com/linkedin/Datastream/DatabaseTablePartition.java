package com.linkedin.datastream;

/**
 * Created by spunuru on 8/10/15.
 */
public class DatabaseTablePartition {
  private String _datastreamName;
  private String _databaseName;
  private String _tableName;
  private int _partition;

  public String getDatastreamName() {
    return _datastreamName;
  }

  public String getDatabaseName() {
    return _databaseName;
  }

  public String getTableName() {
    return _tableName;
  }

  public int getPartition() {
    return _partition;
  }
}
