package com.linkedin.datastream.server;

import java.util.Properties;

import com.linkedin.datastream.server.api.connector.DatastreamDeduper;
import com.linkedin.datastream.server.api.connector.DatastreamDeduperFactory;


public class SourceBasedDeduperFactory implements DatastreamDeduperFactory {
  @Override
  public DatastreamDeduper createDatastreamDeduper(Properties deduperProperties) {
    return new SourceBasedDeduper();
  }
}
