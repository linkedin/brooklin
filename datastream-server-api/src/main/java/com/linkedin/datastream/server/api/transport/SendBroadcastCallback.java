/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.server.api.transport;

import java.util.List;

import com.linkedin.datastream.server.Pair;


/**
 * Callback interface that the connector needs to implement to listen to failures on the send.
 */
public interface SendBroadcastCallback {
  /**
   * Callback method that needs to be called when broadcast completes.
   *
   * If partition count is -1 it indicates that transport provider failed to query partition count. In that case
   * listMetadataExceptionPair contains only one entry with record metadata as null and exception indicating
   * reason for failing to query patition count.
   *
   * @param listMetadataExceptionPair List of Pair of datastream record metadata and exception
   * @param partitionCount Number of topic partitions. -1 when transport provider failed to query partition count.
   */
  void onCompletion(List<Pair<DatastreamRecordMetadata, Exception>> listMetadataExceptionPair, int partitionCount);
}
