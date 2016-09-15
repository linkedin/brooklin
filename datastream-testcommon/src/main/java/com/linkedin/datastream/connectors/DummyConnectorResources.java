package com.linkedin.datastream.connectors;

import com.linkedin.datastream.common.DatastreamRestHandler;
import com.linkedin.restli.server.annotations.Action;
import com.linkedin.restli.server.annotations.ActionParam;
import com.linkedin.restli.server.annotations.RestLiActions;


/**
 * Restli resources implementations for DummyConnector.
 */
@SuppressWarnings("unused")
@RestLiActions(name = "dummy", namespace = "com.linkedin.datastream.server.diagnostics")
public class DummyConnectorResources implements DatastreamRestHandler {
  @Action(name = "echo")
  public String echo(@ActionParam("message") String message) {
    return message;
  }
}
