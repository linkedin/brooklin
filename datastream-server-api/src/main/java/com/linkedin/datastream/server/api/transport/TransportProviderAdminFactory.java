package com.linkedin.datastream.server.api.transport;

import java.util.Properties;


/**
 * Factory to create the Transport provider
 */
public interface TransportProviderAdminFactory {

  /**
   * Create the transport provider admin for the transport provider associated with the transportProvider name.
   * Brooklin will call this to create a TransportProviderAdmin for each of the configured transport providers.
   * @param transportProviderName Name of the transport provider whose admin needs to be created.
   * @param transportProviderProperties Properties corresponding to the transport provider.
   * @return Transport Provider admin instance.
   */
  TransportProviderAdmin createTransportProviderAdmin(String transportProviderName, Properties transportProviderProperties);
}
