package com.linkedin.datastream.server;

import java.util.Collection;

/**
 * Created by pdu on 9/3/15.
 */
public class DataStreamWritableChannelImpl<K, V> implements DataStreamWritableChannel<K, V> {
  @Override
  public void write(DatastreamRecord<K, V> record) {

  }

  @Override
  public void write(Collection<DatastreamRecord<K, V>> records) {

  }
}
