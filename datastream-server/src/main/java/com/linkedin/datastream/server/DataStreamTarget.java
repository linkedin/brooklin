package com.linkedin.datastream.server;

/**
 * Created by pdu on 9/3/15.
 */
public interface DataStreamTarget {
  String getTargetName();
  int getNumPartitions();
  boolean isReusable();
}
