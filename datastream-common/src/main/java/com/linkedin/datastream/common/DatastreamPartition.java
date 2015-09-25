package com.linkedin.datastream.common;

/**
 * Wrapper class with Datastream and partition number.
 */
public class DatastreamPartition {
  private final Datastream _datastream;
  private final Integer _partition;

  public DatastreamPartition(Datastream stream, Integer partition) {
    _datastream = stream;
    _partition = partition;
  }

  public Datastream datastream() {
    return _datastream;
  }

  public Integer partition() {
    return _partition;
  }

  @Override
  public int hashCode() {
    int result = _datastream.hashCode();
    result = 31 * result + _partition.hashCode();
    return result;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;
    DatastreamPartition partition = (DatastreamPartition) o;
    if (!_datastream.equals(partition._datastream))
      return false;
    return _partition.equals(partition._partition);
  }

  @Override
  public String toString() {
    return _datastream.toString() + " partition=" + _partition;
  }
}
