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
   * @param datastreamTask
   * @return
   */
  SerDe assignSerde(DatastreamTask datastreamTask);
}
