/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.common.diag;

import java.util.concurrent.ConcurrentHashMap;

/**
 * PositionDataStore represents a globally-instantiated store of the positions/status of all Connectors on this Brooklin
 * host.
 *
 * The data is stored like this:
 * <ul>
 *   <li>The map's key is the task prefix of the DatastreamTask responsible for managing the consumer</li>
 *   <li>The map's value is a map of the position data</li>
 *   <li><ul>
 *     <li>That map's key is a {@link PositionKey} which uniquely identifies the consumer instance under the
 *         DatastreamTask and the source being consumed from</li>
 *     <li>That map's value is a {@link PositionValue} which describes information on the consumer's position and status
 *         when reading from the source described in the {@link PositionKey}</li>
 *   </ul></li>
 * </ul>
 */
public class PositionDataStore extends ConcurrentHashMap<String, ConcurrentHashMap<PositionKey, PositionValue>> {
  private static final PositionDataStore INSTANCE = new PositionDataStore();
  private static final long serialVersionUID = 1L;

  private PositionDataStore() {
    super();
  }

  /**
   * Returns the globally-instantiated PositionDataStore.
   * @return the globally-instantiated PositionDataStore
   */
  public static PositionDataStore getInstance() {
    return INSTANCE;
  }
}