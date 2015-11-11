package com.linkedin.datastream.server;

import java.util.Map;
import java.util.HashMap;


public class CoordinatorEvent {

  public enum EventName {
    LEADER_DO_ASSIGNMENT,
    HANDLE_ASSIGNMENT_CHANGE,
    HANDLE_NEW_DATASTREAM,
    HANDLE_INSTANCE_ERROR
  }

  private final EventName _eventName;
  private final Map<String, Object> _eventAttributeMap;

  private CoordinatorEvent(EventName eventName) {
    _eventName = eventName;
    _eventAttributeMap = new HashMap<>();
  }

  public static CoordinatorEvent createLeaderDoAssignmentEvent() {
    return new CoordinatorEvent(EventName.LEADER_DO_ASSIGNMENT);
  }

  public static CoordinatorEvent createHandleAssignmentChangeEvent() {
    return new CoordinatorEvent(EventName.HANDLE_ASSIGNMENT_CHANGE);
  }

  public static CoordinatorEvent createHandleNewDatastreamEvent() {
    return new CoordinatorEvent(EventName.HANDLE_NEW_DATASTREAM);
  }

  public static CoordinatorEvent createHandleInstanceErrorEvent(String errorMessage) {
    CoordinatorEvent event = new CoordinatorEvent(EventName.HANDLE_INSTANCE_ERROR);
    event.setAttribute("error_message", errorMessage);
    return event;
  }

  public EventName getName() {
    return _eventName;
  }

  public Object getAttribute(String attribute) {
    return _eventAttributeMap.get(attribute);
  }

  protected void setAttribute(String attribute, Object value) {
    _eventAttributeMap.put(attribute, value);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("name:" + _eventName.toString());

    if (_eventAttributeMap.size() > 0) {
      sb.append("\n");
    }

    for (String key : _eventAttributeMap.keySet()) {
      sb.append(key).append(":").append(_eventAttributeMap.get(key)).append("\n");
    }
    return sb.toString();
  }

}
