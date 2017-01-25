package com.linkedin.datastream.server;

import java.util.Properties;

import com.linkedin.datastream.server.api.transport.TransportProviderAdmin;
import com.linkedin.datastream.server.api.transport.TransportProviderAdminFactory;


public class InMemoryTransportProviderAdminFactory implements TransportProviderAdminFactory {

  @Override
  public TransportProviderAdmin createTransportProviderAdmin(String transportProviderName,
      Properties transportProviderProperties) {
    return new InMemoryTransportProviderAdmin();
  }
}
