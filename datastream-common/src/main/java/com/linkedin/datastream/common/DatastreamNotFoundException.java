package com.linkedin.datastream.common;

/**
 * Exception when the datastream is not found.
 */
public class DatastreamNotFoundException extends DatastreamException {
  public DatastreamNotFoundException(String datastreamName) {
    super(String.format("Datastream %s is not found", datastreamName));
  }
}
