package com.linkedin.datastream.server.dms;

import java.util.List;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.datastream.common.Datastream;
import com.linkedin.datastream.common.DatastreamException;
import com.linkedin.datastream.common.RestliUtils;
import com.linkedin.datastream.server.Coordinator;
import com.linkedin.datastream.server.DatastreamServer;
import com.linkedin.datastream.server.api.connector.DatastreamValidationException;
import com.linkedin.restli.common.HttpStatus;
import com.linkedin.restli.server.CreateResponse;
import com.linkedin.restli.server.PagingContext;
import com.linkedin.restli.server.UpdateResponse;
import com.linkedin.restli.server.annotations.Context;
import com.linkedin.restli.server.annotations.RestLiCollection;
import com.linkedin.restli.server.resources.CollectionResourceTemplate;


/*
 * Resources classes are used by rest.li to process corresponding http request.
 * Note that rest.li will instantiate an object each time it processes a request.
 * So do make it thread-safe when implementing the resources.
 */
@RestLiCollection(name = "datastream", namespace = "com.linkedin.datastream.server.dms")
public class DatastreamResources extends CollectionResourceTemplate<String, Datastream> {
  private static final Logger LOG = LoggerFactory.getLogger(DatastreamResources.class);

  private final DatastreamStore _store;
  private final Coordinator _coordinator;
  private final ErrorLogger _errorLogger;

  public DatastreamResources(DatastreamServer datastreamServer) {
    _store = datastreamServer.getDatastreamStore();
    _coordinator = datastreamServer.getCoordinator();
    _errorLogger = new ErrorLogger(LOG);
  }

  @Override
  public UpdateResponse update(String key, Datastream datastream) {
    // TODO: behavior of updating a datastream is not fully defined yet; block this method for now
    return new UpdateResponse(HttpStatus.S_405_METHOD_NOT_ALLOWED);
  }

  @Override
  public UpdateResponse delete(String key) {
    _store.deleteDatastream(key);
    return new UpdateResponse(HttpStatus.S_200_OK);
  }

  // Returning null will automatically trigger a 404 Not Found response
  @Override
  public Datastream get(String name) {
    return _store.getDatastream(name);
  }

  @SuppressWarnings("deprecated")
  @Override
  public List<Datastream> getAll(@Context PagingContext pagingContext) {
    return RestliUtils.withPaging(_store.getAllDatastreams(), pagingContext)
        .map(_store::getDatastream)
        .filter(stream -> stream != null)
        .collect(Collectors.toList());
  }

  @Override
  public CreateResponse create(Datastream datastream) {
    // rest.li has done this mandatory field check in the latest version.
    // Just in case we roll back to an earlier version, let's do the validation here anyway
    if (!datastream.hasName()) {
      return _errorLogger.logAndGetResponse(HttpStatus.S_400_BAD_REQUEST, "Must specify name of Datastream!");
    }
    if (!datastream.hasConnectorType()) {
      return _errorLogger.logAndGetResponse(HttpStatus.S_400_BAD_REQUEST, "Must specify connectorType!");
    }
    if (!datastream.hasSource()) {
      return _errorLogger.logAndGetResponse(HttpStatus.S_400_BAD_REQUEST, "Must specify source of Datastream!");
    }

    Datastream initializedDatastream;

    try {
      initializedDatastream = _coordinator.initializeDatastream(datastream);
    } catch (DatastreamValidationException e) {
      return _errorLogger.logAndGetResponse(HttpStatus.S_400_BAD_REQUEST, "Failed to initialize Datastream: ", e);
    }

    if (initializedDatastream == null) {
      return _errorLogger.logAndGetResponse(HttpStatus.S_400_BAD_REQUEST,
          "Failed to initialize Datastream, initializeDatastream returned null");
    }

    if (!initializedDatastream.getName().equals(datastream.getName())) {
      return new CreateResponse(initializedDatastream.getName());
    }

    try {
      _store.createDatastream(datastream.getName(), datastream);
      return new CreateResponse(datastream.getName(), HttpStatus.S_201_CREATED);
    } catch (DatastreamException e) {
      return _errorLogger.logAndGetResponse(HttpStatus.S_409_CONFLICT, "Failed to create datastream: " + datastream, e);
    }
  }
}
