package com.linkedin.datastream.server.api.transport;

/**
 * Callback interface that the connector needs to implement to listen to failures on the send.
 */
public interface SendCallback {

  /**
   * Callback method that needs to be called when the send completes
   * @param metadata
   *   Metadata of the Datastream record that got sent.
   * @param exception
   *   null if the send succeeded, Contains the exception if the send failed.
   */
  void onCompletion(DatastreamRecordMetadata metadata, Exception exception);
}
