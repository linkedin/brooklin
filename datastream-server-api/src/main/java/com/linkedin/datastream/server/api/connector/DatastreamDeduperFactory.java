package com.linkedin.datastream.server.api.connector;

import java.util.Properties;


/**
 * Factory for DatastreamDeduper
 */
public interface DatastreamDeduperFactory {

  /**
   * Create datastream deduper
   * @param deduperProperties properties for datastream deduper
   */
  DatastreamDeduper createDatastreamDeduper(Properties deduperProperties);
}
