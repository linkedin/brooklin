package com.linkedin.datastream.server.dms;

import com.linkedin.datastream.common.Datastream;
import com.linkedin.datastream.common.DatastreamException;
import com.linkedin.datastream.server.Coordinator;
import com.linkedin.datastream.server.DatastreamServer;
import com.linkedin.datastream.server.DatastreamValidationResult;
import com.linkedin.datastream.server.zk.ZkClient;
import com.linkedin.restli.common.HttpStatus;
import com.linkedin.restli.server.CreateResponse;
import com.linkedin.restli.server.RestLiServiceException;
import com.linkedin.restli.server.UpdateResponse;
import com.linkedin.restli.server.annotations.Optional;
import com.linkedin.restli.server.annotations.QueryParam;
import com.linkedin.restli.server.annotations.RestLiCollection;
import com.linkedin.restli.server.annotations.RestMethod;
import com.linkedin.restli.server.resources.CollectionResourceTemplate;


/*
 * Resources classes are used by rest.li to process corresponding http request.
 * Note that rest.li will instantiate an object each time it processes a request.
 * So do make it thread-safe when implementing the resources.
 */
@RestLiCollection(name = "datastream", namespace = "com.linkedin.datastream.server.dms")
public class DatastreamResources extends CollectionResourceTemplate<String, Datastream> {

  private final DatastreamStore _store = DatastreamServer.INSTANCE.getDatastreamStore();
  private final Coordinator _coordinator = DatastreamServer.INSTANCE.getCoordinator();

  @Override
  public UpdateResponse update(String key, Datastream datastream) {
    // TODO: behavior of updating a datastream is not fully defined yet; block this method for now
    return new UpdateResponse(HttpStatus.S_405_METHOD_NOT_ALLOWED);
  }

  @Override
  public UpdateResponse delete(String key) {
    // TODO: behavior of deleting a datastream is not fully defined yet; block this method for now
    return new UpdateResponse(HttpStatus.S_405_METHOD_NOT_ALLOWED);
  }

  // Returning null will automatically trigger a 404 Not Found response
  @Override
  public Datastream get(String name) {
    return _store.getDatastream(name);
  }

  @Override
  public CreateResponse create(Datastream datastream) {
    // rest.li has done this mandatory field check in the latest version.
    // Just in case we roll back to an earlier version, let's do the validation here anyway
    if (!datastream.hasName()) {
      return new CreateResponse(new RestLiServiceException(HttpStatus.S_400_BAD_REQUEST,
          "Must specify name of Datastream!"));
    }
    if (!datastream.hasConnectorType()) {
      return new CreateResponse(new RestLiServiceException(HttpStatus.S_400_BAD_REQUEST,
          "Must specify connectorType!"));
    }
    if (!datastream.hasSource()) {
      return new CreateResponse(new RestLiServiceException(HttpStatus.S_400_BAD_REQUEST,
          "Must specify source of Datastream!"));
    }

    DatastreamValidationResult validation = _coordinator.validateDatastream(datastream);
    if (!validation.getSuccess()) {
      return new CreateResponse(new RestLiServiceException(HttpStatus.S_400_BAD_REQUEST,
          validation.getErrorMsg()));
    }

    if (_store.createDatastream(datastream.getName(), datastream)) {
      return new CreateResponse("Datastream created", HttpStatus.S_201_CREATED);
    } else {
      return new CreateResponse(new RestLiServiceException(HttpStatus.S_409_CONFLICT));
    }
  }
}
