package com.linkedin.datastream.server;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;

import org.apache.commons.lang.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javafx.util.Pair;

import com.linkedin.datastream.common.AvroUtils;
import com.linkedin.datastream.common.DatastreamEvent;
import com.linkedin.datastream.common.ErrorLogger;


/**
 * Builder class for DatastreamProducerRecord
 */
public class DatastreamProducerRecordBuilder {

  private static final Logger LOG = LoggerFactory.getLogger(DatastreamProducerRecordBuilder.class.getName());

  private Optional<Integer> _partition = Optional.empty();
  private String _sourceCheckpoint = "";

  private List<Pair<byte[], byte[]>> _events = new ArrayList<>();

  /**
   * Partition to which this DatastreamProducerRecord should be produced. if the partition is not set, TransportProvider
   * is expected to use key to send the event to appropriate partition. In this case, It is possible that
   * events within the DatastreamProducerRecord will be sent to different partitions.
   */
  public void setPartition(int partition) {
    Validate.isTrue(partition >= 0, "invalid partition number: " + String.valueOf(partition));
    _partition = Optional.of(partition);
  }

  /**
   * Set the sourcecheckpoint for the datastream record.
   * @param sourceCheckpoint
   */
  public void setSourceCheckpoint(String sourceCheckpoint) {
    _sourceCheckpoint = sourceCheckpoint;
  }

  /**
   * Add the event with key and value to the DatatreamProducerRecord. Datastream producer record can have multiple events.
   */
  public void addEvent(byte[] key, byte[] value) {
    key = key == null ? new byte[0] : key;
    value = value == null ? new byte[0] : value;
    _events.add(new Pair<>(key, value));
  }

  /**
   * Add the DatastreamEvent to the producer record.
   * @param datastreamEvent
   *   DatastreamEvent that needs to be added.
   */
  public void addEvent(DatastreamEvent datastreamEvent) {
    Validate.notNull(datastreamEvent);

    datastreamEvent.metadata = datastreamEvent.metadata == null ? new HashMap<>() : datastreamEvent.metadata;
    datastreamEvent.key = datastreamEvent.key == null ? ByteBuffer.allocate(0) : datastreamEvent.key;
    datastreamEvent.payload = datastreamEvent.payload == null ? ByteBuffer.allocate(0) : datastreamEvent.payload;
    datastreamEvent.previous_payload = datastreamEvent.previous_payload == null ? ByteBuffer.allocate(0) : datastreamEvent.previous_payload;

    byte[] event = null;
    try {
      event = AvroUtils.encodeAvroSpecificRecord(DatastreamEvent.class, datastreamEvent);
    } catch (IOException e) {
      ErrorLogger.logAndThrowDatastreamRuntimeException(LOG, "Failed to encode event in Avro, event=" + datastreamEvent, e);
    }

    _events.add(new Pair<>(datastreamEvent.key.array(), event));
  }

  /**
   * Build the DatastreamProducerRecord.
   * @return
   *   DatastreamProducerRecord that is created.
   */
  public DatastreamProducerRecord build() {
    return new DatastreamProducerRecord(_events, _partition, _sourceCheckpoint);
  }
}
