package com.linkedin.datastream.server.dms;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.datastream.common.DatastreamRestHandler;
import com.linkedin.datastream.common.DatastreamRuntimeException;
import com.linkedin.datastream.common.ReflectionUtils;
import com.linkedin.datastream.server.DatastreamServer;
import com.linkedin.restli.internal.server.model.ResourceModel;
import com.linkedin.restli.server.annotations.RestLiActions;
import com.linkedin.restli.server.annotations.RestLiAssociation;
import com.linkedin.restli.server.annotations.RestLiCollection;
import com.linkedin.restli.server.annotations.RestLiSimpleResource;
import com.linkedin.restli.server.resources.ResourceFactory;


/**
 * Datastream Resource Factory that is used to create the datastream restli resources.
 */
public class DatastreamResourceFactory implements ResourceFactory {
  private static final Logger LOG = LoggerFactory.getLogger(DatastreamResourceFactory.class);

  @SuppressWarnings("rawtypes")
  private static final Class [] RESTLI_ANNOTATION_TYPES = {
      RestLiCollection.class,
      RestLiActions.class,
      RestLiSimpleResource.class,
      RestLiAssociation.class,
      RestLiActions.class
  };

  private final DatastreamServer _datastreamServer;
  private final Map<String, DatastreamRestHandler> _restHandlers = new HashMap<>();
  private final Set<String> _restPackages = new HashSet<>();

  public DatastreamResourceFactory(DatastreamServer datastreamServer) {
    _datastreamServer = datastreamServer;
  }

  @SuppressWarnings("unchecked")
  private boolean isValidRestliHandler(DatastreamRestHandler handler) {
    return Arrays.stream(RESTLI_ANNOTATION_TYPES)
        .filter(a -> handler.getClass().isAnnotationPresent(a))
        .findFirst().isPresent();
  }

  public void addRestPackage(String restPackage) {
    if (restPackage != null) {
      _restPackages.add(restPackage);
    }
  }

  public void addRestHandlers(List<DatastreamRestHandler> handlers) {
    if (handlers == null) {
      return;
    }
    for (DatastreamRestHandler handler : handlers) {
      if (!isValidRestliHandler(handler)) {
        String errMsg = "REST handler has no Rest.li annotation: " + handler;
        LOG.error(errMsg);
        throw new DatastreamRuntimeException(errMsg);
      }
      _restHandlers.put(handler.getClass().getCanonicalName(), handler);
      _restPackages.add(handler.getClass().getPackage().getName());
    }
  }

  public Set<String> getRestPackages() {
    return Collections.unmodifiableSet(_restPackages);
  }

  @Override
  public void setRootResources(Map<String, ResourceModel> rootResources) {
  }

  @SuppressWarnings("unchecked")
  @Override
  public <R> R create(Class<R> resourceClass) {
    String className = resourceClass.getCanonicalName();
    DatastreamRestHandler handler = _restHandlers.getOrDefault(className, null);
    if (handler == null) {
      return ReflectionUtils.createInstance(className, _datastreamServer);
    } else {
      return (R) handler;
    }
  }
}
