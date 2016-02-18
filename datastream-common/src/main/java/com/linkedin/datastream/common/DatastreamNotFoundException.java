package com.linkedin.datastream.common;

/**
 * Exception when the datastream is not found.
 */
public class DatastreamNotFoundException extends DatastreamRuntimeException {
  public DatastreamNotFoundException(String datastreamName, Throwable e) {
    super(String.format("Datastream %s is not found", datastreamName), e);
  }
}
