package com.linkedin.datastream.testutil.event.generator;

// import org.slf4j.Logger;
// import org.slf4j.LoggerFactory;
import java.io.File;
import java.io.IOException;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import org.apache.avro.generic.GenericRecord;

import com.linkedin.datastream.common.DatastreamEvent;

/**
 * The class is for generating datastream events.
 * todo - right now, it returns a list of generated events. Need to support publishing to kafka topic directly
 */

public class DatastreamEventGenerator extends GenericEventGenerator {

  // public final static String MODULE = DatastreamEventGenerator.class.getName();
  // public final static Logger LOG = Logger.getLogger(MODULE);
  private boolean _needPreviousPayload;
  private String _datastreamName;
  private DatastreamEvent _lastInsertedEvent = null;
  private DatastreamEvent _lastUpdatedEvent = null;

  /**
   * Takes a schema file as input
   * @param schemaFile
   * @throws IOException
   */
  public DatastreamEventGenerator(File schemaFile) throws IOException {
    super(schemaFile);
    setDefaults();
  }

  /**
   * Takes a schema string as an input
   * @param schema
   */
  public DatastreamEventGenerator(String schema) {
    super(schema);
    setDefaults();
  }

  public void setDefaults() {
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
    metaData.put("Database", _dbName);
    metaData.put("Table", _tableName);
    metaData.put("Partition", String.valueOf(partNum));
    metaData.put("Scn", String.valueOf(_startScn));
    metaData.put("EventTimestamp", String.valueOf(System.currentTimeMillis()));
    metaData.put("__OpCode", opcode);
    // OpCode is set at the caller level.
    // not set  "IsCrossColoReplicated": "false", "SchemaId": ""
    return metaData;
  }

  // right now, using startScn as the key also for easier verification. In future, we will randomize it or have key schema
  private ByteBuffer getKey() {
    ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
    buffer.putLong(_startScn);
    return buffer;
  }

  private ByteBuffer getPayload() throws UnknownTypeException, IOException {
    return getNextEvent();
  }

  @Override
  protected Object getNextEvent(EventType eventType, int partNum) throws UnknownTypeException, IOException {
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

    } else {  // default event
      datastreamEvent.key = getKey();
      opcode = "INSERT";
      datastreamEvent.previous_payload = null;
      _lastInsertedEvent = datastreamEvent;

    }
    datastreamEvent.metadata = getMetaData(partNum, opcode); // get common meta data for this sequence
    return datastreamEvent;
  }

}
