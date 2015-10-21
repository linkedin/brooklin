package com.linkedin.datastream.server.dms;

import com.linkedin.datastream.common.Datastream;
import com.linkedin.datastream.common.DatastreamException;
import com.linkedin.datastream.server.Coordinator;
import com.linkedin.datastream.server.DatastreamServer;
import com.linkedin.datastream.server.DatastreamValidationResult;
import com.linkedin.restli.common.HttpStatus;
import com.linkedin.restli.server.RestLiServiceException;
import com.linkedin.restli.server.annotations.Action;
import com.linkedin.restli.server.annotations.ActionParam;
import com.linkedin.restli.server.annotations.RestLiActions;

@RestLiActions(name = "bootstrap", namespace = "com.linkedin.datastream.server.dms")
public class BootstrapActionResources {

  private final DatastreamStore _store = DatastreamServer.INSTANCE.getDatastreamStore();
  private final Coordinator _coordinator = DatastreamServer.INSTANCE.getCoordinator();

  @Action(name="create")
  public Datastream create(@ActionParam("baseDatastream") String baseDatastreamName)
  {
    Datastream baseDatastream = _store.getDatastream(baseDatastreamName);
    if (baseDatastream == null) {
      throw new RestLiServiceException(HttpStatus.S_404_NOT_FOUND, "Can't create bootstrap datastream. Base datastream not exists.");
    }
    Datastream bootstrapDatastream = new Datastream();
    bootstrapDatastream.setName(baseDatastream.getName() + "-" + System.currentTimeMillis());
    DatastreamServer datastreamServer = DatastreamServer.INSTANCE;
    try {
      String bootstrapConnectorType = datastreamServer.getBootstrapConnector(baseDatastream.getConnectorType());
      bootstrapDatastream.setConnectorType(bootstrapConnectorType);
    } catch (DatastreamException e) {
      throw new RestLiServiceException(HttpStatus.S_406_NOT_ACCEPTABLE, e);
    }
    bootstrapDatastream.setSource(baseDatastream.getSource());
    DatastreamValidationResult validation = _coordinator.validateDatastream(bootstrapDatastream);
    if (!validation.getSuccess()) {
      throw new RestLiServiceException(HttpStatus.S_406_NOT_ACCEPTABLE,
          validation.getErrorMsg());
    }

    if (!_store.createDatastream(bootstrapDatastream.getName(), bootstrapDatastream)) {
      throw new RestLiServiceException(HttpStatus.S_406_NOT_ACCEPTABLE,
          "Failed to create bootstrap datastream.");
    }
    return bootstrapDatastream;
  }
}
