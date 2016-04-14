package com.linkedin.datastream.testutil.event.generator;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;

import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.Encoder;


/**
 * Generator for generic events based on input schema. It allows for customization
 * by overriding {@link GenericEventGenerator#getNextEvent()}.
 */
public class GenericEventGenerator {

  protected enum EventType {
    INSERT,
    UPDATE,
    DELETE_ON_INSERT,
    DELETE_ON_UPDATE,
    CONTROL
  }

  private Schema _schema;
  // db name and partition name
  protected String _dbName = "test_db"; // for the time, take them as input (could take their schemas at some point)
  protected String _tableName = "test_table";
  protected long _startScn = 1;
  private int _numPartitions = 1; // defaults for now
  private int _maxTransactionSize = 1;
  private int _percentageUpdates = 0; // percentage of inserts that will be updates - should be 0 - 50
  private int _percentageDeletes = 0; // percentage of inserts that will be deletes - should be 0 - 50
  private int _percentageControls = 0; // percentage of numOfEvents that will be controls - should be 0 - 50
  private boolean _generateEventFile = false;

  /*
   * Takes a schema file as input
   */
  public GenericEventGenerator(File schemaFile) throws IOException {
    _schema = Schema.parse(schemaFile);
  }

  /*
   * Takes a schema string as an input
   */
  public GenericEventGenerator(String schema) {
    _schema = Schema.parse(schema);
  }

  public GenericEventGenerator() {
    // Subclass generator needs to override getNextEvent
  }

  // set seed for random generator - actual setting happens only once before calling any event generation
  // this is useful to reproduce the test data for debugging.
  public void setSeed(long seed) {
    if (!SchemaField.isSeedSet()) { // seed not set
      SchemaField.setSeed(seed);
      // log the value so that it could be used to reproduce it if needed.
    } else {
      // log the message that the seed given is ignored
    }
  }

  /**
   * Set the maximum number of elements when generating arrays, maps, bytes as well as string.
   * In the case of a string this would be the maximum size of the string.
   * @param maxNumElements maximum number of elements to be set
   */
  public void setMaxNumElements(int maxNumElements) {
    SchemaField.setMaxNumElements(maxNumElements);
  }

  public void setDbName(String dbName) {
    _dbName = dbName;
  }

  public void setTableName(String tableName) {
    _tableName = tableName;
  }

  public void setStartScn(long startScn) {
    _startScn = startScn;
  }

  public void setNumPartitions(int numPartitions) {
    _numPartitions = numPartitions;
  }

  public void setMaxTransactionSize(int maxTransactionSize) {
    _maxTransactionSize = maxTransactionSize;
  }

  public void setPercentageUpdates(int percentageUpdates) {
    if ((percentageUpdates < 0) || ((percentageUpdates + _percentageDeletes) > 50)) {
      // log the error and not set it
      return;
    }
    _percentageUpdates = percentageUpdates;
  }

  public void setPercentageDeletes(int percentageDeletes) {
    if ((percentageDeletes < 0) || ((_percentageUpdates + percentageDeletes) > 50)) {
      // log the error and not set it
      return;
    }
    _percentageDeletes = percentageDeletes;
  }

  public void setPercentageControls(int percentageControls) {
    if ((percentageControls < 0) || (percentageControls > 50)) {
      // log the error that it needs to be between 0 - 50
      return;
    }
    _percentageControls = percentageControls;
  }

  protected GenericRecord getNextGenericRecord() throws UnknownTypeException {
    GenericRecord record = new GenericData.Record(_schema);
    for (Schema.Field field : _schema.getFields()) {
      SchemaField schemaFill = SchemaField.createField(field);
      schemaFill.writeToRecord(record);
    }
    return record;
  }

  protected ByteBuffer getNextEvent() throws UnknownTypeException, IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    Encoder e = new BinaryEncoder(out);
    GenericDatumWriter<GenericRecord> w = new GenericDatumWriter<GenericRecord>(_schema);
    GenericRecord nextRecord = getNextGenericRecord();
    w.write(nextRecord, e);
    e.flush();
    ByteBuffer serialized = ByteBuffer.allocate(out.toByteArray().length);
    serialized.put(out.toByteArray());
    return serialized;
  }

  // expected to be overriden in derived classed
  protected Object getNextEvent(EventType eventType, int partNum) throws UnknownTypeException, IOException {
    // at this level, we don't have any wrapper on top of data event. So, just create a random event based on the given schema and return it
    return getNextGenericRecord();
  }

  /*
   * Generate random Espresso Events based on the avro schema
   * The schema must be of a record type to work as well as GenericEvent type
   *
   * @return returns the list of generated records
   */
  public List<Object> generateGenericEventList(int numEvents) throws UnknownTypeException, IOException {

    if (_schema != null && _schema.getType() != Schema.Type.RECORD) {
      // LOG.error("The schema first level must be record.");
      return null;
    }

    // todo - compute min and max inserts needed and pick a random num between them and then compute updates-on-updates,
    //        updates-on-inserts, so on. In that sense, updates can go up to 99%, but deletes can't go beyond 50%
    int numControls = numEvents * _percentageControls / 100;
    int numDataEvents = numEvents - numControls;
    int numUpdates = numDataEvents * _percentageUpdates / 100;
    int numDeletes = numDataEvents * _percentageDeletes / 100;
    int numInserts = numDataEvents - numUpdates - numDeletes;
    // assert((numInserts + numUpdates + numDeletes) == numDataEvents);
    // assert(numInserts > max(numUpdates, numDeletes));
    int numDeletesOnUpdates = numDeletes * numUpdates / numDataEvents;
    int numDeletesOnInserts = numDeletes - numDeletesOnUpdates;
    int controlIndex = (numControls > 0) ? (numEvents / numControls) : (numEvents + 1);
    long lastInsertScn = -1;
    long lastUpdateScn = -1;
    int curPartition = 0;

    List<Object> eventList = new ArrayList<>();
    // todo - randomizing it truly and guaranteeing that every event happens
    //        mean while we are using a simple logic to determine whether it is an insert, update or delete
    //        update and deletes need preimage - so we choose them only when that condition met, otherwise, it is insert
    for (int i = 0; i < numEvents; ++i) {
      EventType eventType = EventType.INSERT;
      if (((i + 1) % controlIndex) == 0) { // transition event
        eventType = EventType.CONTROL; // make it a transition
        --numControls;
      } else if ((lastUpdateScn >= 0) && (numDeletesOnUpdates > 0)) {
        eventType = EventType.DELETE_ON_UPDATE; // make it a delete on update
        --numDeletesOnUpdates;
        lastUpdateScn = -1;
      } else if ((lastInsertScn >= 0) && ((numDeletesOnInserts > 0) || (numUpdates > 0))) {
        // todo - make it probabilistically either a delete on insert or an update
        if ((((i % 2) == 1) || (numDeletesOnInserts == 0)) && (numUpdates > 0)) {
          eventType = EventType.UPDATE; // make it an update
          lastUpdateScn = _startScn;
          --numUpdates;
        } else {
          // assert (numDeletesOnInserts > 0)
          eventType = EventType.DELETE_ON_INSERT; // make it a delete on insert
          --numDeletesOnInserts;
        }
        lastInsertScn = -1;
      } else if (numInserts > 0) {
        eventType = EventType.INSERT;
        lastInsertScn = _startScn;
        --numInserts;
      } else {
        eventType = EventType.CONTROL; // make it a transition - this is from the remainder part
        --numControls;
      }
      Object genericEvent = getNextEvent(eventType, curPartition++);
      eventList.add(genericEvent);
      if (curPartition >= _numPartitions) {
        curPartition = 0;
      }
      ++_startScn;
    }
    return eventList;
  }
}
