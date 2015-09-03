package com.linkedin.datastream.server;

public class DatastreamChannelFactory {
  public static DatastreamWritableChannel createChannel(DatastreamTarget target) {
    return new DataStreamWritableChannelImpl(); // TODO
  }
}
