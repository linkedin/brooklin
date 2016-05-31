package com.linkedin.datastream.server.api.connector;

import java.util.Properties;

/**
 * Connector factory interface, Each connector should implement this which creates the connector instance.
 */
public interface ConnectorFactory {

  /**
   * create connector instance. Each connector should implement this method to create the corresponding connector
   * instance based on the configuration.
   * @param config
   *   Connector configuration.
   * @return
   *    Instance of the connector that is created.
   */
  public Connector createConnector(Properties config);
}
