/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.server.dms;

import java.util.stream.Stream;

import com.linkedin.datastream.common.Datastream;
import com.linkedin.datastream.common.DatastreamException;


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
   * The result should be sorted so that consumers can implement paging correctly.
   * @return
   */
  Stream<String> getAllDatastreams();

  /**
   * Updates the datastream associated with the given key with the provided one.
   * @param key datastream name of the original datastream to be updated
   * @param datastream content of the updated datastream
   * @param notifyLeader whether to notify leader about the update
   * @throws DatastreamException
   */
  void updateDatastream(String key, Datastream datastream, boolean notifyLeader) throws DatastreamException;

  /**
   * Creates a new datastream and associates it with the provided key.
   * @param key
   * @param datastream
   * @throws DatastreamException
   */
  void createDatastream(String key, Datastream datastream);

  /**
   * Deletes the datastream associated with the provided key.
   * @param key
   */
  void deleteDatastream(String key);
}
