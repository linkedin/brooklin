package com.linkedin.datastream.server.dms;

import java.util.Map;

import com.linkedin.datastream.common.ReflectionUtils;
import com.linkedin.datastream.server.DatastreamServer;
import com.linkedin.restli.internal.server.model.ResourceModel;
import com.linkedin.restli.server.resources.ResourceFactory;


/**
 * Datastream Resource Factory that is used to create the datastream restli resources.
 */
public class DatastreamResourceFactory implements ResourceFactory {

  private final DatastreamServer _datastreamServer;

  public DatastreamResourceFactory(DatastreamServer datastreamServer) {
    _datastreamServer = datastreamServer;
  }

  @Override
  public void setRootResources(Map<String, ResourceModel> rootResources) {
  }

  @Override
  public <R> R create(Class<R> resourceClass) {
    return ReflectionUtils.createInstance(resourceClass.getCanonicalName(), _datastreamServer);
  }
}
