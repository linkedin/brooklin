package com.linkedin.datastream.server.transport;

import com.linkedin.datastream.server.DatastreamProducerRecord;
import com.linkedin.datastream.server.api.transport.DatastreamRecordMetadata;
import com.linkedin.datastream.server.api.transport.SendCallback;
import com.linkedin.datastream.server.api.transport.TransportProvider;


public class NoOpTransportProvider implements TransportProvider {

  @Override
  public void send(String destination, DatastreamProducerRecord record, SendCallback onComplete) {
    DatastreamRecordMetadata metadata =  new DatastreamRecordMetadata(
        record.getCheckpoint(), null, record.getPartition().orElse(null));
    onComplete.onCompletion(metadata, null);
  }

  @Override
  public void close() {
  }

  @Override
  public void flush() {
  }
}
