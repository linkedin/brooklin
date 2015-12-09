package com.linkedin.datastream.server.api.transport;

import java.util.Properties;

/**
 * Factory to create the Transport provider
 */
public interface TransportProviderFactory {

  /**
   * Every TransportProvider should implement this factory method to create the instance of the TransportProvider that
   * Datastream Server will use the TransportProviderFactory to create the transportProvider that is configured to use.
   * @param transportProviderProperties
   *   Properties to configure the TransportProvider
   * @return
   *   TransportProvider.
   */
  TransportProvider createTransportProvider(Properties transportProviderProperties);
}
