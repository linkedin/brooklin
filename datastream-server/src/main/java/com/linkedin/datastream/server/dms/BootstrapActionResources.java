package com.linkedin.datastream.server.dms;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.datastream.common.Datastream;
import com.linkedin.datastream.server.Coordinator;
import com.linkedin.datastream.server.DatastreamServer;
import com.linkedin.restli.common.HttpStatus;
import com.linkedin.restli.server.annotations.Action;
import com.linkedin.restli.server.annotations.ActionParam;
import com.linkedin.restli.server.annotations.RestLiActions;


/**
 * BootstrapActionResources is the rest end point to process bootstrap datastream request
 */
@RestLiActions(name = "bootstrap", namespace = "com.linkedin.datastream.server.dms")
public class BootstrapActionResources {
  private static final Logger LOG = LoggerFactory.getLogger(BootstrapActionResources.class);

  private final DatastreamStore _store;
  private final Coordinator _coordinator;
  private final DatastreamServer _datastreamServer;
  private final ErrorLogger _errorLogger;

  public BootstrapActionResources(DatastreamServer datastreamServer) {
    _datastreamServer = datastreamServer;
    _store = datastreamServer.getDatastreamStore();
    _coordinator = datastreamServer.getCoordinator();
    _errorLogger = new ErrorLogger(LOG);
  }

  /**
   * Process the request of creating bootstrap datastream. The request provides the bootstrap datastream
   * that needs to be created. Server will either return an existing datastream or create a new bootstrap datastream
   * based on the request.
   */
  @Action(name = "create")
  public Datastream create(@ActionParam("boostrapDatastream") Datastream bootstrapDatastream) {

    LOG.info(String.format("Create bootstrap datastream called with datastream %s", bootstrapDatastream));

    if (!bootstrapDatastream.hasName()) {
      _errorLogger.logAndThrowRestLiServiceException(HttpStatus.S_400_BAD_REQUEST, "Must specify name of Datastream!");
    }

    if (!bootstrapDatastream.hasConnectorType()) {
      _errorLogger.logAndThrowRestLiServiceException(HttpStatus.S_400_BAD_REQUEST, "Must specify connectorType!");
    }
    if (!bootstrapDatastream.hasSource()) {
      _errorLogger.logAndThrowRestLiServiceException(HttpStatus.S_400_BAD_REQUEST,
          "Must specify source of Datastream!");
    }

    try {
      _coordinator.initializeDatastream(bootstrapDatastream);
      _store.createDatastream(bootstrapDatastream.getName(), bootstrapDatastream);
    } catch (Exception e) {
      _errorLogger.logAndThrowRestLiServiceException(HttpStatus.S_500_INTERNAL_SERVER_ERROR,
          String.format("Failed to initialize bootstrap Datastream %s", bootstrapDatastream), e);
    }

    return bootstrapDatastream;
  }
}
