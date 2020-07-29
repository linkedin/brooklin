/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.common;

/**
 * Callback interface that the connector needs to implement to listen to failures on the send.
 */
public interface SendCallback {

  /**
   * Callback method that needs to be called when the send completes
   * @param metadata
   *   Metadata of the Datastream record that got sent. Could be null if an exception occurred based on the transport
   * @param exception
   *   null if the send succeeded, Contains the exception if the send failed.
   */
  void onCompletion(DatastreamRecordMetadata metadata, Exception exception);
}
