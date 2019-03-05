/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.server.api.serde;

import com.linkedin.datastream.serde.SerDe;
import com.linkedin.datastream.server.DatastreamTask;


/**
 * SerdeAdmin interface that each Serde needs to implement.
 */
public interface SerdeAdmin {

  /**
   * Assign a serde for the datastream task. SerdeAdmin can decide to store the SerDe
   * and reuse it across datastream tasks or create a new one for every datastreamTask.
   */
  SerDe assignSerde(DatastreamTask datastreamTask);
}
