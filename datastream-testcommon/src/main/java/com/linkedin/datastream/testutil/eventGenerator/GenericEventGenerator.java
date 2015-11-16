package com.linkedin.datastream.testutil.eventGenerator;

import java.io.File;
import java.io.IOException;

// import org.apache.log4j.Logger;
import java.lang.String;
import java.util.ArrayList;
import java.util.List;
import org.apache.avro.Schema;
// import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

import com.linkedin.datastream.testutil.common.RandomValueGenerator;


/*
 * The class is a factory which returns an instance based on which random data can be written to the record
 */
public class GenericEventGenerator {

  // public final static String MODULE = GenericEventGenerator.class.getName();
  // public final static Logger LOG = Logger.getLogger(MODULE);

  private enum EventType {
    INSERT,
    UPDATE,
    DELETE_ON_INSERT,
    DELETE_ON_UPDATE,
    CONTROL
  }

  private Schema _schema;
  // db name and partition name
  protected String _dbName; // for the time, take them as input (could take their schemas at some point)
  protected String _partName;
  protected long _startScn = 0;
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

  public void setDbName(String dbName) {
    _dbName = dbName;
  }

  public void setPartName(String partName) {
    _partName = partName;
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

  // expected to be overriden in derived classed
  protected GenericRecord getNextEvent(EventType eventType, int partNum) throws UnknownTypeException {
    // at this level, we don't have any wrapper on top of data event. So, just create a random event based on the given schema and return it
    return getNextGenericRecord();
  }

  /*
   * Generate random Espresso Events based on the avro schema
   * The schema must be of a record type to work as well as GenericEvent type
   *
   * @return returns the list of generated records
   */
  public List<GenericRecord> generateGenericEventList(int numEvents) throws UnknownTypeException {

    if (_schema.getType() != Schema.Type.RECORD) {
      // LOG.error("The schema first level must be record.");
      return null;
    }

    int numControls = numEvents * _percentageControls / 100;
    int numDataEvents = numEvents - numControls;
    int numUpdates = numDataEvents * _percentageUpdates / 100;
    int numDeletes = numDataEvents * _percentageDeletes / 100;
    int numInserts = numDataEvents - numUpdates - numDeletes;
    // assert((numInserts + numUpdates + numDeletes) == numDataEvents);
    // assert(numInserts > max(numUpdates, numDeletes));
    int numDeletesOnUpdates = numDeletes * numUpdates / numDataEvents;
    int numDeletesOnInserts = numDeletes - numDeletesOnUpdates;
    int maxUpdateRange = numInserts / Math.max(numDeletes, numUpdates);
    int controlIndex = (numControls > 0) ? (numEvents / numControls) : (numEvents + 1);
    long lastInsertScn = -1;
    long lastUpdateScn = -1;
    int curPartition = 0;
    // rowImage for insert
    // rowImage for update

    List<GenericRecord> eventList = new ArrayList<>();
    // todo - randomizing it truly and guaranteeing that every event happens
    for (int i = 0; i < numEvents; ++i) {
      EventType eventType = EventType.INSERT;
      if (((i + 1) % controlIndex) == 0) { // transition event
        eventType = EventType.CONTROL; // make it a transition
        --numControls;
      } else if ((lastUpdateScn >= 0) && (numDeletesOnUpdates > 0)) {
        eventType = EventType.DELETE_ON_UPDATE; // make it a delete on update
        --numDeletesOnUpdates;
      } else if ((lastInsertScn >= 0) && ((numDeletesOnInserts > 0) || (numUpdates > 0))) {
        // todo - make it probabilistically either a delete on insert or an update
        if ((((i % 2) == 1) || (numDeletesOnInserts == 0)) && (numUpdates > 0)) {
          eventType = EventType.UPDATE; // make it an update
          --numUpdates;
        } else {
          // assert (numDeletesOnInserts > 0)
          eventType = EventType.DELETE_ON_INSERT; // make it a delete on insert
          --numDeletesOnInserts;
        }
      } else if (numInserts > 0) {
        eventType = EventType.INSERT;
        --numInserts;
      } else {
        eventType = EventType.CONTROL; // make it a transition - this is from the remainder part
        --numControls;
      }
      GenericRecord genericEvent = getNextEvent(eventType, curPartition++);
      eventList.add(genericEvent);
      if (curPartition >= _numPartitions)
        curPartition = 0;
    }
    return eventList;
  }
}
