/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.testutil.event.generator;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import org.apache.avro.Schema;

import com.linkedin.datastream.common.DatastreamEvent;


/**
 * The class is for generating datastream events.
 * todo - right now, it returns a list of generated events. Need to support publishing to kafka topic directly
 */

public class DatastreamEventGenerator extends AbstractEventGenerator<DatastreamEvent> {

  private boolean _needPreviousPayload;
  private String _datastreamName;
  private DatastreamEvent _lastInsertedEvent = null;
  private DatastreamEvent _lastUpdatedEvent = null;

  public DatastreamEventGenerator(Schema schema, EventGeneratorConfig cfg) {
    super(schema, cfg);

    _needPreviousPayload = true;
    _datastreamName = "testDatastream";
  }

  public void setNeedPreviousPayload(boolean needPreviousPayload) {
    _needPreviousPayload = needPreviousPayload;
  }

  public void setDatastreamName(String datastreamName) {
    _datastreamName = datastreamName;
  }

  private Map<CharSequence, CharSequence> getMetaData(int partNum, String opcode) {
    Map<CharSequence, CharSequence> metaData = new HashMap<>();
    metaData.put("Database", _cfg.getDbName());
    metaData.put("Table", _cfg.getTableName());
    metaData.put("Partition", String.valueOf(partNum));
    metaData.put("Scn", String.valueOf(_scn));
    metaData.put("EventTimestamp", String.valueOf(System.currentTimeMillis()));
    metaData.put("__OpCode", opcode);
    // OpCode is set at the caller level.
    // not set  "IsCrossColoReplicated": "false", "SchemaId": ""
    return metaData;
  }

  // right now, using startScn as the key also for easier verification. In future, we will randomize it or have key schema
  private ByteBuffer getKey() {
    ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
    buffer.putLong(_scn);
    return buffer;
  }

  private ByteBuffer getPayload() throws UnknownTypeException, IOException {
    return getNextEvent();
  }

  @Override
  protected DatastreamEvent getNextEvent(EventType eventType, int partNum) throws UnknownTypeException, IOException {
    DatastreamEvent datastreamEvent = new DatastreamEvent();
    datastreamEvent.payload = getPayload();
    datastreamEvent.previous_payload = null;
    DatastreamEvent preEvent = null;
    String opcode = null;

    switch (eventType) {
      case UPDATE:
        opcode = "UPDATE";
        preEvent = _lastInsertedEvent;
        _lastUpdatedEvent = datastreamEvent;
        // get the key from the saved insert
        // if previous payload, set it
        // set the previous update pointer
        break;
      case DELETE_ON_INSERT:
        opcode = "DELETE";
        preEvent = _lastInsertedEvent;
        _lastInsertedEvent = null;
        break;
      case DELETE_ON_UPDATE:
        opcode = "DELETE";
        preEvent = _lastUpdatedEvent;
        _lastUpdatedEvent = null;
        break;
      default: // intentional fall-through
      case CONTROL: // intentional fall-through
        // not supposed to hit this - log the error and treat it as if insert
      case INSERT:
        // nothing to do here as it is default behavior
        break;
    }

    if (preEvent != null) {
      datastreamEvent.key = preEvent.key;
      if (_needPreviousPayload) {
        datastreamEvent.previous_payload = preEvent.payload;
      }
    } else { // default event
      datastreamEvent.key = getKey();
      opcode = "INSERT";
      datastreamEvent.previous_payload = null;
      _lastInsertedEvent = datastreamEvent;
    }
    datastreamEvent.metadata = getMetaData(partNum, opcode); // get common meta data for this sequence
    return datastreamEvent;
  }
}
