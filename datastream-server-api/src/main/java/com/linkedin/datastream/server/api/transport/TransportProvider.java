package com.linkedin.datastream.server.api.transport;

import com.linkedin.datastream.metrics.MetricsAware;
import com.linkedin.datastream.server.DatastreamProducerRecord;


/**
 * Datastream is transport agnostic system, This is the interface that each TransportProvider needs to be implement
 * to plug the different transport mechanisms (Kafka, kinesis, etc..) to Datastream
 */
public interface TransportProvider extends MetricsAware {
  /**
   * Send the DatastreamEvent to the topic.
   * @param destination the destination topic to which the record should be sent.
   * @param record DatastreamEvent that needs to be sent to the stream.
   * @param onComplete call back that needs to called when the send completes.
   */
  void send(String destination, DatastreamProducerRecord record, SendCallback onComplete);

  /**
   * Closes the transport provider and its corresponding producer.
   */
  void close();

  /**
   * Flush to make sure that the current set of events that are in the buffer gets flushed to the server.
   */
  void flush();
}
