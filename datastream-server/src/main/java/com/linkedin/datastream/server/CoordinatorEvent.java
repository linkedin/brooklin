package com.linkedin.datastream.server;

public class CoordinatorEvent {

  public enum EventType {
    LEADER_DO_ASSIGNMENT,
    HANDLE_ASSIGNMENT_CHANGE,
    HANDLE_ADD_OR_DELETE_DATASTREAM,
    HANDLE_INSTANCE_ERROR
  }

  public static final CoordinatorEvent LEADER_DO_ASSIGNMENT_EVENT = new CoordinatorEvent(EventType.LEADER_DO_ASSIGNMENT);
  public static final CoordinatorEvent HANDLE_ASSIGNMENT_CHANGE_EVENT = new CoordinatorEvent(EventType.HANDLE_ASSIGNMENT_CHANGE);
  public static final CoordinatorEvent HANDLE_ADD_OR_DELETE_DATASTREAM_EVENT = new CoordinatorEvent(EventType.HANDLE_ADD_OR_DELETE_DATASTREAM);

  protected final EventType _eventType;

  private CoordinatorEvent(EventType eventType) {
    _eventType = eventType;
  }

  public EventType getType() {
    return _eventType;
  }

  @Override
  public String toString() {
    return "type:" + _eventType;
  }

  public static CoordinatorEvent createLeaderDoAssignmentEvent() {
    return LEADER_DO_ASSIGNMENT_EVENT;
  }

  public static CoordinatorEvent createHandleAssignmentChangeEvent() {
    return HANDLE_ASSIGNMENT_CHANGE_EVENT;
  }

  public static CoordinatorEvent createHandleDatastreamAddOrDeleteEvent() {
    return HANDLE_ADD_OR_DELETE_DATASTREAM_EVENT;
  }

  public static HandleInstanceError createHandleInstanceErrorEvent(String errorMessage) {
    return new HandleInstanceError(errorMessage);
  }

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
