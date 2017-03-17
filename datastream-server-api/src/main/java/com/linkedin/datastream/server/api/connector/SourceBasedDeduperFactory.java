package com.linkedin.datastream.server.api.connector;

import java.util.Properties;


public class SourceBasedDeduperFactory implements DatastreamDeduperFactory {
  @Override
  public DatastreamDeduper createDatastreamDeduper(Properties deduperProperties) {
    return new SourceBasedDeduper();
  }
}
