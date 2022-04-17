/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.server.api.transport;

import java.util.List;
import java.util.Optional;

import com.linkedin.datastream.server.Pair;


/**
 * Callback interface that the connector needs to implement to listen to failures on the send.
 */
public interface SendBroadcastCallback {
  /**
   * Callback method that needs to be called when broadcast completes. Datastream record metadata will be empty
   * if we fail to extract partition count.
   * @param failedToSendRecord List of Pair of datastream record metadata and exception
   */
  void onCompletion(List<Pair<Optional<DatastreamRecordMetadata>, Exception>> failedToSendRecord);
}
