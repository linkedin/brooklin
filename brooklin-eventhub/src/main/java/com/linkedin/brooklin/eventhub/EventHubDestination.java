package com.linkedin.brooklin.eventhub;

import java.net.URI;

import com.linkedin.data.template.StringMap;
import com.linkedin.datastream.common.Datastream;


public class EventHubDestination {

  public static final String SHARED_ACCESS_KEY_NAME = "sharedAccessKeyName";
  public static final String SHARED_ACCESS_KEY = "sharedAccessKey";

  public EventHubDestination() {
  }

  public EventHubDestination(Datastream datastream) {
    URI destinationUri = URI.create(datastream.getDestination().getConnectionString());
    _eventHubNamespace = destinationUri.getAuthority();
    _eventHubName = destinationUri.getPath().substring(1);
    StringMap datastreamMetadata = datastream.getMetadata();
    _sharedAccessKey = datastreamMetadata.get(SHARED_ACCESS_KEY);
    _sharedAccessKeyName = datastreamMetadata.get(SHARED_ACCESS_KEY_NAME);
  }

  private String _eventHubNamespace;

  private String _eventHubName;

  private String _sharedAccessKeyName;

  private String _sharedAccessKey;

  public String getEventHubNamespace() {
    return _eventHubNamespace;
  }

  public void setEventHubNamespace(String eventHubNamespace) {
    _eventHubNamespace = eventHubNamespace;
  }

  public String getEventHubName() {
    return _eventHubName;
  }

  public void setEventHubName(String eventHubName) {
    _eventHubName = eventHubName;
  }

  public String getSharedAccessKeyName() {
    return _sharedAccessKeyName;
  }

  public void setSharedAccessKeyName(String sharedAccessKeyName) {
    _sharedAccessKeyName = sharedAccessKeyName;
  }

  public String getSharedAccessKey() {
    return _sharedAccessKey;
  }

  public void setSharedAccessKey(String sharedAccessKey) {
    _sharedAccessKey = sharedAccessKey;
  }
}
