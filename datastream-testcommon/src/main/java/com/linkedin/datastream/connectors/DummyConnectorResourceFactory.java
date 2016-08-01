package com.linkedin.datastream.connectors;

import java.util.Map;

import com.linkedin.restli.internal.server.model.ResourceModel;
import com.linkedin.restli.server.resources.ResourceFactory;

public class DummyConnectorResourceFactory implements ResourceFactory {
  public DummyConnectorResourceFactory() {
  }

  @Override
  public void setRootResources(Map<String, ResourceModel> rootResources) {

  }

  @SuppressWarnings("unchecked")
  @Override
  public <R> R create(Class<R> resourceClass) {
    return (R) new DummyConnectorResources();
  }
}
