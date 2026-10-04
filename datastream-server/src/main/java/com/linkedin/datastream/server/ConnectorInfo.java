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

  private final ConnectorWrapper _connector;

  private final AssignmentStrategy _assignmentStrategy;

  private final boolean _customCheckpointing;

  private final DatastreamDeduper _datastreamDeduper;

  private final CheckpointProvider _checkpointProvider;

  /**
   * Store authorizerName because authorizer might be initialized later
   */
  private final Optional<String> _authorizerName;

  private final boolean _byotGroupJoinAllowed;

  /**
   * Constructor for ConnectorInfo, which does not allow BYOT group joins
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
    this(name, connector, strategy, customCheckpointing, checkpointProvider, deduper, authorizerName, false);
  }

  /**
   * Constructor for ConnectorInfo
   * @param name Connector name
   * @param connector Connector object
   * @param strategy Assignment strategy associated with {@code connector}
   * @param customCheckpointing true if {@code connector} uses custom checkpointing
   * @param checkpointProvider Checkpoint provider associated with {@code connector}
   * @param deduper Datastream deduper associated with {@code connector}
   * @param authorizerName Name of the authorizer configured by {@code connector} (if any)
   * @param byotGroupJoinAllowed true if a BYOT datastream of {@code connector} may join the BYOT group that already
   *                             uses its destination
   */
  public ConnectorInfo(String name, Connector connector, AssignmentStrategy strategy, boolean customCheckpointing,
      CheckpointProvider checkpointProvider, DatastreamDeduper deduper, String authorizerName,
      boolean byotGroupJoinAllowed) {
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
    _byotGroupJoinAllowed = byotGroupJoinAllowed;
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

  public boolean isByotGroupJoinAllowed() {
    return _byotGroupJoinAllowed;
  }
}
