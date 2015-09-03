package com.linkedin.datastream.server;

import java.util.Collection;

public interface DataStreamWritableChannel<K, V> {
  void write(DatastreamRecord<K, V> record);
  void write(Collection<DatastreamRecord<K, V>> records);
}
