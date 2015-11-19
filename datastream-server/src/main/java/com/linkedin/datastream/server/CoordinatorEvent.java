package com.linkedin.datastream.server;


import java.util.List;

public class CoordinatorEvent<T> {

  public enum EventType {
    LEADER_DO_ASSIGNMENT,
    HANDLE_ASSIGNMENT_CHANGE,
    HANDLE_NEW_DATASTREAM,
    HANDLE_INSTANCE_ERROR
  }

  private final EventType _eventType;
  private T _eventData;

  private CoordinatorEvent(EventType eventName) {
    _eventType = eventName;
    _eventData = null;
  }

  private CoordinatorEvent(EventType eventName, T eventData) {
    _eventType = eventName;
    _eventData = eventData;
  }

  public static CoordinatorEvent createLeaderDoAssignmentEvent() {
    return new CoordinatorEvent(EventType.LEADER_DO_ASSIGNMENT);
  }

  public static CoordinatorEvent createHandleAssignmentChangeEvent() {
    return new CoordinatorEvent(EventType.HANDLE_ASSIGNMENT_CHANGE);
  }

  public static CoordinatorEvent createHandleNewDatastreamEvent() {
    return new CoordinatorEvent(EventType.HANDLE_NEW_DATASTREAM);
  }

  public static CoordinatorEvent<String> createHandleInstanceErrorEvent(String errorMessage) {
    CoordinatorEvent event = new CoordinatorEvent(EventType.HANDLE_INSTANCE_ERROR, errorMessage);
    return event;
  }

  public EventType getType() {
    return _eventType;
  }

  public T getEventData() {
    return _eventData;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("type:" + _eventType.toString());

    if (_eventData != null) {
      sb.append("\n");
      sb.append(_eventData);
    }

    return sb.toString();
  }

}
