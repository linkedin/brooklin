package com.linkedin.datastream.server;

import java.util.Properties;

import com.linkedin.datastream.server.api.transport.TransportProvider;
import com.linkedin.datastream.server.api.transport.TransportProviderFactory;


public class InMemoryTransportProviderFactory implements TransportProviderFactory {
  private static InMemoryTransportProvider _transportProvider = new InMemoryTransportProvider();

  @Override
  public TransportProvider createTransportProvider(Properties transportProviderProperties) {
    return _transportProvider;
  }

  public static InMemoryTransportProvider getTransportProvider() {
    return _transportProvider;
  }
}
