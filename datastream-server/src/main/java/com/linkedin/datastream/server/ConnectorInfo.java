package com.linkedin.datastream.server;

import com.linkedin.datastream.server.api.connector.Connector;
import com.linkedin.datastream.server.api.connector.DatastreamDeduper;
import com.linkedin.datastream.server.api.strategy.AssignmentStrategy;


/**
 * Metadata related to the connector.
 */
public class ConnectorInfo {

  private String _name;

  private ConnectorWrapper _connector;

  private AssignmentStrategy _assignmentStrategy;

  private boolean _customCheckpointing;

  private DatastreamDeduper _datastreamDeduper;

  public ConnectorInfo(String name, Connector connector, AssignmentStrategy strategy, boolean customCheckpointing,
      DatastreamDeduper deduper) {
    _name = name;
    _connector = new ConnectorWrapper(name, connector);
    _assignmentStrategy = strategy;
    _customCheckpointing = customCheckpointing;
    _datastreamDeduper = deduper;
  }

  public ConnectorWrapper getConnector() {
    return _connector;
  }

  public AssignmentStrategy getAssignmentStrategy() {
    return _assignmentStrategy;
  }

  public boolean isCustomCheckpointing() {
    return _customCheckpointing;
  }

  public DatastreamDeduper getDatastreamDeduper() {
    return _datastreamDeduper;
  }

  public String getConnectorType() {
    return _connector.getConnectorType();
  }
}
