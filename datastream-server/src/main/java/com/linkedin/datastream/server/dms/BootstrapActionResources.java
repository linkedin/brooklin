package com.linkedin.datastream.server.dms;

import com.linkedin.datastream.common.Datastream;
import com.linkedin.datastream.common.DatastreamException;
import com.linkedin.datastream.server.Coordinator;
import com.linkedin.datastream.server.DatastreamServer;
import com.linkedin.datastream.server.api.connector.DatastreamValidationException;
import com.linkedin.restli.common.HttpStatus;
import com.linkedin.restli.server.annotations.Action;
import com.linkedin.restli.server.annotations.ActionParam;
import com.linkedin.restli.server.annotations.RestLiActions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


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
   * Process the request of creating bootstrap datastream. The request provides the name of
   * base datastream, and the server will create and return a corresponding bootstrap
   * datastream.
   */
  @Action(name = "create")
  public Datastream create(@ActionParam("baseDatastream") String baseDatastreamName) {
    Datastream baseDatastream = _store.getDatastream(baseDatastreamName);
    if (baseDatastream == null) {
      _errorLogger.logAndThrow(HttpStatus.S_404_NOT_FOUND, "Base datastream does not exists.");
    }
    Datastream bootstrapDatastream = new Datastream();
    bootstrapDatastream.setName(String.format("%s-Bootstrap-%d", baseDatastream.getName(),
            System.currentTimeMillis()));
    try {
      String bootstrapConnectorType = _datastreamServer.getBootstrapConnector(baseDatastream.getConnectorType());
      bootstrapDatastream.setConnectorType(bootstrapConnectorType);
    } catch (DatastreamException e) {
      _errorLogger.logAndThrow(HttpStatus.S_400_BAD_REQUEST,
              "Missing bootstrap connector for " + baseDatastream.getConnectorType());
    }
    bootstrapDatastream.setSource(baseDatastream.getSource());
    try {
      _coordinator.initializeDatastream(bootstrapDatastream);
    } catch (DatastreamValidationException e) {
      _errorLogger.logAndThrow(HttpStatus.S_400_BAD_REQUEST, "Failed to initialize " + bootstrapDatastream, e);
    }

    try {
      _store.createDatastream(bootstrapDatastream.getName(), bootstrapDatastream);
    } catch (DatastreamException e) {
      _errorLogger.logAndThrow(HttpStatus.S_500_INTERNAL_SERVER_ERROR,
              "Failed to initialize " + bootstrapDatastream, e);
    }

    return bootstrapDatastream;
  }
}
