/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.server;

import java.util.Optional;

import org.apache.commons.lang.StringUtils;

import com.linkedin.datastream.server.api.connector.Connector;
import com.linkedin.datastream.server.api.connector.DatastreamDeduper;
import com.linkedin.datastream.server.api.strategy.AssignmentStrategy;
import com.linkedin.datastream.server.providers.CheckpointProvider;


/**
 * Metadata related to the connector.
 */
public class ConnectorInfo {

  private ConnectorWrapper _connector;

  private AssignmentStrategy _assignmentStrategy;

  private boolean _customCheckpointing;

  private DatastreamDeduper _datastreamDeduper;

  private CheckpointProvider _checkpointProvider;

  /**
   * Store authorizerName because authorizer might be initialized later
   */
  private Optional<String> _authorizerName;

  /**
   * Constructor for ConnectorInfo
   * @param name Connector name
   * @param connector Connector object
   * @param strategy Assignment strategy associated with {@code connector}
   * @param customCheckpointing true if {@code connector} uses custom checkpointing
   * @param checkpointProvider Checkpoint provider associated with {@code connector}
   * @param deduper Datastream deduper associated with {@code connector}
   * @param authorizerName Name of the authorizer configured by {@code connector} (if any)
   */
  public ConnectorInfo(String name, Connector connector, AssignmentStrategy strategy, boolean customCheckpointing,
      CheckpointProvider checkpointProvider, DatastreamDeduper deduper, String authorizerName) {
    _connector = new ConnectorWrapper(name, connector);
    _assignmentStrategy = strategy;
    _customCheckpointing = customCheckpointing;
    _datastreamDeduper = deduper;
    _checkpointProvider = checkpointProvider;
    if (StringUtils.isBlank(authorizerName)) {
      _authorizerName = Optional.empty();
    } else {
      _authorizerName = Optional.of(authorizerName);
    }
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

  public Optional<String> getAuthorizerName() {
    return _authorizerName;
  }

  public CheckpointProvider getCheckpointProvider() {
    return _checkpointProvider;
  }
}
