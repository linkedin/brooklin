package com.linkedin.datastream.server;

public enum DatastreamTaskStatus {
  OK,
  ERROR;

  public static boolean isOK(DatastreamTaskStatus status) {
    return OK == status;
  }

  public static boolean isFailure(DatastreamTaskStatus status) {
    return ERROR == status;
  }
}
