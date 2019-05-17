/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.common.diag;

import java.util.concurrent.ConcurrentHashMap;

/**
 * ConnectorPositionsCache represents a globally-instantiated store of the positions/status of all Connectors on this
 * Brooklin host.
 *
 * The data is stored like this:
 * <ul>
 *   <li>The map's key is the name of the Connector</li>
 *   <li>The map's value is a map of the position data
 *     <ul>
 *       <li>That map's key is a {@link PositionKey} which uniquely identifies the consumer instance under the
 *           DatastreamTask and the source being consumed from</li>
 *       <li>That map's value is a {@link PositionValue} which describes information on the consumer's position and status
 *           when reading from the source described in the {@link PositionKey}</li>
 *     </ul>
 *   </li>
 * </ul>
 */
public class ConnectorPositionsCache extends ConcurrentHashMap<String, ConcurrentHashMap<PositionKey, PositionValue>> {
  private static final ConnectorPositionsCache INSTANCE = new ConnectorPositionsCache();
  private static final long serialVersionUID = 1L;

  private ConnectorPositionsCache() {
    super();
  }

  /**
   * Returns the globally-instantiated ConnectorPositionsCache.
   * @return the globally-instantiated ConnectorPositionsCache
   */
  public static ConnectorPositionsCache getInstance() {
    return INSTANCE;
  }
}