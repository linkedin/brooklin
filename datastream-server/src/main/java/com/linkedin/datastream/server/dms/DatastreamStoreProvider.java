package com.linkedin.datastream.server.dms;

// TODO when we have a 'datastream server manager', we can move this logic there
// Provide static method to get DatastreamStore instance
public class DatastreamStoreProvider {
  private static DatastreamStore _instance = null;

  public static DatastreamStore getDatastreamStore() {
    if (_instance == null) {
      throw new RuntimeException("Datastream Store instance not initialized.");
    }
    return _instance;
  }

  public static void setDatastreamStore(DatastreamStore store) {
    _instance = store;
  }
}
