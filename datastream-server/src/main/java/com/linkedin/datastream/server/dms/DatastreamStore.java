package com.linkedin.datastream.server.dms;

import com.linkedin.datastream.common.Datastream;


/**
 * A key value store interface for Datastream that supports CREATE, READ, UPDATE, and DELETE
 */
public interface DatastreamStore {
  public Datastream getDatastream(String key);
  public boolean updateDatastream(String key, Datastream datastream);
  public boolean createDatastream(String key, Datastream datastream);
  public boolean deleteDatastream(String key);
}
