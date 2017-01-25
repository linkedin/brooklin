package com.linkedin.brooklin.samza;

import java.util.Properties;

import com.linkedin.datastream.server.api.transport.TransportProviderAdmin;
import com.linkedin.datastream.server.api.transport.TransportProviderAdminFactory;


public class SamzaTransportProviderAdminFactory implements TransportProviderAdminFactory {

  @Override
  public TransportProviderAdmin createTransportProviderAdmin(String transportProviderName,
      Properties transportProviderProperties) {
    return new SamzaTransportProviderAdmin(transportProviderName, transportProviderProperties);
  }
}
