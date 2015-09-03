package com.linkedin.datastream.server;

import java.util.Collection;

public class DataStreamWritableChannelImpl implements DatastreamWritableChannel {
  @Override
  public <K, V> void write(DatastreamRecord<K, V> record) {
    // TODO
  }

  @Override
  public <K, V> void write(Collection<DatastreamRecord<K, V>> records) {
    // TODO
  }
}
