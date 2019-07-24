/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.server;

import java.util.Optional;


/**
 * Represents different event types inside {@link Coordinator}.
 */
public class CoordinatorEvent {

  /**
   * Represents event types inside {@link Coordinator}.
   */
  public enum EventType {
    LEADER_DO_ASSIGNMENT,
    LEADER_PARTITION_ASSIGNMENT,
    HANDLE_ASSIGNMENT_CHANGE,
    HANDLE_DATASTREAM_CHANGE_WITH_UPDATE,
    HANDLE_ADD_OR_DELETE_DATASTREAM,
    HANDLE_INSTANCE_ERROR,
    HEARTBEAT
  }

  public static final CoordinatorEvent LEADER_DO_ASSIGNMENT_EVENT =
      new CoordinatorEvent(EventType.LEADER_DO_ASSIGNMENT);
  public static final CoordinatorEvent HANDLE_ASSIGNMENT_CHANGE_EVENT =
      new CoordinatorEvent(EventType.HANDLE_ASSIGNMENT_CHANGE);
  public static final CoordinatorEvent HANDLE_DATASTREAM_CHANGE_WITH_UPDATE_EVENT =
      new CoordinatorEvent(EventType.HANDLE_DATASTREAM_CHANGE_WITH_UPDATE);
  public static final CoordinatorEvent HANDLE_ADD_OR_DELETE_DATASTREAM_EVENT =
      new CoordinatorEvent(EventType.HANDLE_ADD_OR_DELETE_DATASTREAM);
  public static final CoordinatorEvent HEARTBEAT_EVENT = new CoordinatorEvent(EventType.HEARTBEAT);
  protected final EventType _eventType;

  protected final Optional<String> _datastreamGroupName;

  private CoordinatorEvent(EventType eventType) {
    _eventType = eventType;
    _datastreamGroupName = Optional.empty();
  }

  private CoordinatorEvent(EventType eventType, String datastreamGroupName) {
    _eventType = eventType;
    _datastreamGroupName = Optional.ofNullable(datastreamGroupName);
  }

  /**
   * Returns an event that indicates a new assignment needs to be done (this is a leader-specific event).
   */
  public static CoordinatorEvent createLeaderDoAssignmentEvent() {
    return LEADER_DO_ASSIGNMENT_EVENT;
  }

  /**
   * Returns an event that indicates a change in task assignment to connectors.
   */
  public static CoordinatorEvent createHandleAssignmentChangeEvent() {
    return HANDLE_ASSIGNMENT_CHANGE_EVENT;
  }

  /**
   * Returns an event that indicates an update to a datastream was made.
   */
  public static CoordinatorEvent createHandleDatastreamChangeEvent() {
    return HANDLE_DATASTREAM_CHANGE_WITH_UPDATE_EVENT;
  }

  /**
   * Retrun an event that indicates that partition need to be assigned for a datastream group
   * @param datastreamGroupName the name of datastream group which receives partition changes
   * @return
   */
  public static CoordinatorEvent createLeaderPartitionAssignmentEvent(String datastreamGroupName) {
    return new CoordinatorEvent(EventType.LEADER_PARTITION_ASSIGNMENT, datastreamGroupName);
  }

  /**
   * Returns an event that indicates addition/deletion of new/existing datastream.
   */
  public static CoordinatorEvent createHandleDatastreamAddOrDeleteEvent() {
    return HANDLE_ADD_OR_DELETE_DATASTREAM_EVENT;
  }

  /**
   * Creates an instance of HandleInstanceError that represents an error event inside coordinator.
   * @param errorMessage Error message associated with the event.
   */
  public static HandleInstanceError createHandleInstanceErrorEvent(String errorMessage) {
    return new HandleInstanceError(errorMessage);
  }

  public Optional<String> getDatastreamGroupName() {
    return _datastreamGroupName;
  }

  public EventType getType() {
    return _eventType;
  }

  @Override
  public String toString() {
    return "type:" + _eventType;
  }

  /**
   * Represents an error event seen inside coordinator. It stores the error event along with the error message.
   */
  public static final class HandleInstanceError extends CoordinatorEvent {
    private final String _errorMessage;

    private HandleInstanceError(String errorMessage) {
      super(EventType.HANDLE_INSTANCE_ERROR);
      _errorMessage = errorMessage;
    }

    public String getEventData() {
      return _errorMessage;
    }

    @Override
    public String toString() {
      return "type:" + _eventType + "\n" + _errorMessage;
    }
  }
}
