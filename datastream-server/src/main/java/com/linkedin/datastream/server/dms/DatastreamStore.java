package com.linkedin.datastream.server.dms;

import com.linkedin.datastream.common.Datastream;

import java.util.stream.Stream;


/**
 * A key value store interface for Datastream that supports CREATE, READ, UPDATE, and DELETE
 */
public interface DatastreamStore {
  /**
   * Retrieves the datastream associated with the given key.
   * @param key
   * @return
   */
  Datastream getDatastream(String key);

  /**
   * Retrieves all the datastreams in the store. Since there may be many datastreams, it is better
   * to return a Stream and enable further filtering and transformation rather that just a List.
   * @return
   */
  Stream<Datastream> getAllDatastreams();

  /**
   * Updates the datastream associated with the given key with the provided one.
   * @param key
   * @param datastream
   * @return
   */
  boolean updateDatastream(String key, Datastream datastream);

  /**
   * Creates a new datastream and associates it with the provided key.
   * @param key
   * @param datastream
   * @return
   */
  boolean createDatastream(String key, Datastream datastream);

  /**
   * Deletes the datastream associated with the provided key.
   * @param key
   * @return
   */
  boolean deleteDatastream(String key);
}
