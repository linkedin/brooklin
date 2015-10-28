package com.linkedin.datastream.server;

import java.util.Map;
import java.util.HashMap;

public class CoordinatorEvent {

  public enum EventName {
    LEADER_DO_ASSIGNMENT,
    HANDLE_ASSIGNMENT_CHANGE
  }

  private final EventName _eventName;
  private final Map<String, Object> _eventAttributeMap;

  public CoordinatorEvent(EventName eventName) {
    _eventName = eventName;
    _eventAttributeMap = new HashMap<>();
  }

  public EventName getName() {
    return _eventName;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("name:" + _eventName.toString());

    if (_eventAttributeMap.size() > 0) {
      sb.append("\n");
    }

    for(String key : _eventAttributeMap.keySet()) {
      sb.append(key).append(":").append(_eventAttributeMap.get(key)).append("\n");
    }
    return sb.toString();
  }

}
