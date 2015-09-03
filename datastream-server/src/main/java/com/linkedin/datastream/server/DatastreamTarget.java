package com.linkedin.datastream.server;

public interface DatastreamTarget {
  String getTargetName();
  int getNumPartitions();
  boolean isReusable();
}
