package com.linkedin.brooklin.eventhub;

import java.util.Properties;

import com.linkedin.datastream.server.api.transport.TransportProviderAdmin;
import com.linkedin.datastream.server.api.transport.TransportProviderAdminFactory;


public class EventHubTransportProviderAdminFactory implements TransportProviderAdminFactory {

  @Override
  public TransportProviderAdmin createTransportProviderAdmin(String transportProviderName,
      Properties transportProviderProperties) {

    return new EventHubTransportProviderAdmin(transportProviderName, transportProviderProperties);
  }
}
