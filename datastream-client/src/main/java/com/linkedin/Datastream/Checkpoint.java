package com.linkedin.datastream;

/**
 * Created by spunuru on 8/10/15.
 */
public class Checkpoint {
  private DatabaseTablePartition _databaseTablePartition;
  private Long _offset;

  public Checkpoint(DatabaseTablePartition dtp, long offset, int i) {

  }

  public DatabaseTablePartition getDatabaseTablePartition() {
    return _databaseTablePartition;
  }

  public Long getOffset() {
    return _offset;
  }
}
