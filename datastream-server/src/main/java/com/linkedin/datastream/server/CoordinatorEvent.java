/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.server;

/**
 * Class that represents different event types inside coordinator.
 */
public class CoordinatorEvent {

  /**
   * Enum that represents event types inside coordinator.
   */
  public enum EventType {
    LEADER_DO_ASSIGNMENT,
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

  private CoordinatorEvent(EventType eventType) {
    _eventType = eventType;
  }

  /**
   * Returns LEADER_DO_ASSIGNMENT_EVENT that represents new assignment needs to be done (this is a leader
   * specific event).
   * @return Coordinator event LEADER_DO_ASSIGNMENT_EVENT.
   */
  public static CoordinatorEvent createLeaderDoAssignmentEvent() {
    return LEADER_DO_ASSIGNMENT_EVENT;
  }

  /**
   * Returns HANDLE_ASSIGNMENT_CHANGE_EVENT that represents change in task assignment to connectors.
   * @return Coordinator event HANDLE_ASSIGNMENT_CHANGE_EVENT.
   */
  public static CoordinatorEvent createHandleAssignmentChangeEvent() {
    return HANDLE_ASSIGNMENT_CHANGE_EVENT;
  }

  /**
   * Returns HANDLE_DATASTREAM_CHANGE_WITH_UPDATE_EVENT that represents update to a datastream.
   * @return Coordinator event HANDLE_DATASTREAM_CHANGE_WITH_UPDATE_EVENT.
   */
  public static CoordinatorEvent createHandleDatastreamChangeEvent() {
    return HANDLE_DATASTREAM_CHANGE_WITH_UPDATE_EVENT;
  }

  /**
   * Returns HANDLE_ADD_OR_DELETE_DATASTREAM_EVENT that represents addition/deletion of new datastream.
   * @return Coordinator event HANDLE_ADD_OR_DELETE_DATASTREAM_EVENT.
   */
  public static CoordinatorEvent createHandleDatastreamAddOrDeleteEvent() {
    return HANDLE_ADD_OR_DELETE_DATASTREAM_EVENT;
  }

  /**
   * Creates an instance of HandleInstanceError that represents an error event inside coordinator.
   * @param errorMessage Error message associated with the event.
   * @return An instance of HandleInstanceError that represents error event.
   */
  public static HandleInstanceError createHandleInstanceErrorEvent(String errorMessage) {
    return new HandleInstanceError(errorMessage);
  }

  public EventType getType() {
    return _eventType;
  }

  @Override
  public String toString() {
    return "type:" + _eventType;
  }

  /**
   * Class that represents an error event seen inside coorodinator.It stores the error event along with error message.
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
