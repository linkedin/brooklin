package com.linkedin.datastream.server;

import java.util.Collection;

public interface DatastreamWritableChannel {
  <K, V> void write(DatastreamRecord<K, V> record);
  <K, V> void write(Collection<DatastreamRecord<K, V>> records);
}
