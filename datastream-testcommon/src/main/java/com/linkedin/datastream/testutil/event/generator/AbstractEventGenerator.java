/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.testutil.event.generator;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;


/**
 * Generator for generic events based on input schema.
 * @param <T> the type of record to generate
 */
public abstract class AbstractEventGenerator<T extends IndexedRecord> {

  protected enum EventType {
    INSERT,
    UPDATE,
    DELETE_ON_INSERT,
    DELETE_ON_UPDATE,
    CONTROL
  }

  protected final Schema _schema;
  protected final EventGeneratorConfig _cfg;
  protected long _scn;

  /**
   * Constructor for AbstractEventGenerator
   * @param schema the schema of the events to generate
   * @param cfg the event generator config
   */
  public AbstractEventGenerator(Schema schema, EventGeneratorConfig cfg) {
    _schema = schema;
    _cfg = cfg;
    _scn = cfg.getStartScn();
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
    Encoder e = EncoderFactory.get().binaryEncoder(out, null);
    GenericDatumWriter<GenericRecord> w = new GenericDatumWriter<>(_schema);
    GenericRecord nextRecord = getNextGenericRecord();
    w.write(nextRecord, e);
    e.flush();
    ByteBuffer serialized = ByteBuffer.allocate(out.toByteArray().length);
    serialized.put(out.toByteArray());
    return serialized;
  }

  abstract protected T getNextEvent(EventType eventType, int partNum) throws UnknownTypeException, IOException;

  /**
   * @return a generic event iterator
   */
  public Iterator<T> genericEventIterator() {
    if (_schema != null && _schema.getType() != Schema.Type.RECORD) {
      // The schema first level must be record.
      return null;
    }

    return new GenericEventIterator();
  }

  /*
   * Generate random events based on the Avro schema. The schema must be of a record type to work as well as
   * GenericEvent type
   * @return returns the list of generated records
   */
  public List<T> generateGenericEventList(int numEvents) throws UnknownTypeException, IOException {

    if (_schema != null && _schema.getType() != Schema.Type.RECORD) {
      // The schema first level must be record.
      return null;
    }

    int numControls = numEvents * _cfg.getPercentageControls() / 100;
    int numDataEvents = numEvents - numControls;
    int numUpdates = numDataEvents * _cfg.getPercentageUpdates() / 100;
    int numDeletes = numDataEvents * _cfg.getPercentageDeletes() / 100;
    int numInserts = numDataEvents - numUpdates - numDeletes;
    int numDeletesOnUpdates = numDeletes * numUpdates / numDataEvents;
    int numDeletesOnInserts = numDeletes - numDeletesOnUpdates;
    int controlIndex = (numControls > 0) ? (numEvents / numControls) : (numEvents + 1);
    long lastInsertScn = -1;
    long lastUpdateScn = -1;
    int curPartition = 0;

    List<T> eventList = new ArrayList<>();
    for (int i = 0; i < numEvents; ++i) {
      EventType eventType;
      if (((i + 1) % controlIndex) == 0) { // transition event
        eventType = EventType.CONTROL; // make it a transition
        --numControls;
      } else if ((lastUpdateScn >= 0) && (numDeletesOnUpdates > 0)) {
        eventType = EventType.DELETE_ON_UPDATE; // make it a delete on update
        --numDeletesOnUpdates;
        lastUpdateScn = -1;
      } else if ((lastInsertScn >= 0) && ((numDeletesOnInserts > 0) || (numUpdates > 0))) {
        if ((((i % 2) == 1) || (numDeletesOnInserts == 0)) && (numUpdates > 0)) {
          eventType = EventType.UPDATE; // make it an update
          lastUpdateScn = _scn;
          --numUpdates;
        } else {
          // assert (numDeletesOnInserts > 0)
          eventType = EventType.DELETE_ON_INSERT; // make it a delete on insert
          --numDeletesOnInserts;
        }
        lastInsertScn = -1;
      } else if (numInserts > 0) {
        eventType = EventType.INSERT;
        lastInsertScn = _scn;
        --numInserts;
      } else {
        eventType = EventType.CONTROL; // make it a transition - this is from the remainder part
        --numControls;
      }
      T genericEvent = getNextEvent(eventType, curPartition++);
      eventList.add(genericEvent);
      if (curPartition >= _cfg.getNumPartitions()) {
        curPartition = 0;
      }
      _scn++;
    }
    return eventList;
  }

  /**
   * Generates random events based on the given Schema and configuration options.
   */
  public static class GenericEventGenerator extends AbstractEventGenerator<GenericRecord> {

    /**
     * Constructor for GenericEventGenerator
     */
    public GenericEventGenerator(Schema schema, EventGeneratorConfig cfg) {
      super(schema, cfg);
    }

    @Override
    protected GenericRecord getNextEvent(EventType eventType, int partNum) throws UnknownTypeException {
      // at this level, we don't have any wrapper on top of data event. So, just create a random event based on the
      // given schema and return it
      return getNextGenericRecord();
    }
  }

  /**
   * Holds configuration options for EventGenerators.
   */
  public static class EventGeneratorConfig {
    // db name and partition name
    protected String _dbName = "test_db"; // for the time, take them as input (could take their schemas at some point)
    protected String _tableName = "test_table";
    protected long _startScn = 1;
    protected int _numPartitions = 1; // defaults for now
    protected int _maxTransactionSize = 1;
    protected int _percentageUpdates = 0; // percentage of inserts that will be updates - should be 0 - 50
    protected int _percentageDeletes = 0; // percentage of inserts that will be deletes - should be 0 - 50
    protected int _percentageControls = 0; // percentage of numOfEvents that will be controls - should be 0 - 50
    protected boolean _generateEventFile = false;

    /**
     * Set seed for random generator - actual setting happens only once before calling any event generation. This is
     * useful to reproduce the test data for debugging.
     */
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

    public String getDbName() {
      return _dbName;
    }

    public void setDbName(String dbName) {
      _dbName = dbName;
    }

    public String getTableName() {
      return _tableName;
    }

    public void setTableName(String tableName) {
      _tableName = tableName;
    }

    public long getStartScn() {
      return _startScn;
    }

    public void setStartScn(long startScn) {
      _startScn = startScn;
    }

    public int getNumPartitions() {
      return _numPartitions;
    }

    public void setNumPartitions(int numPartitions) {
      _numPartitions = numPartitions;
    }

    public int getMaxTransactionSize() {
      return _maxTransactionSize;
    }

    public void setMaxTransactionSize(int maxTransactionSize) {
      _maxTransactionSize = maxTransactionSize;
    }

    public int getPercentageUpdates() {
      return _percentageUpdates;
    }

    /**
     * Set the percentage of events that should be updates.
     */
    public void setPercentageUpdates(int percentageUpdates) {
      if ((percentageUpdates < 0) || ((percentageUpdates + _percentageDeletes) > 50)) {
        return;
      }
      _percentageUpdates = percentageUpdates;
    }

    public int getPercentageDeletes() {
      return _percentageDeletes;
    }

    /**
     * Set the percentage of events that should be deletes.
     */
    public void setPercentageDeletes(int percentageDeletes) {
      if ((percentageDeletes < 0) || ((_percentageUpdates + percentageDeletes) > 50)) {
        // log the error and not set it
        return;
      }
      _percentageDeletes = percentageDeletes;
    }

    public int getPercentageControls() {
      return _percentageControls;
    }

    /**
     * Set the percentage of events that should be control events.
     */
    public void setPercentageControls(int percentageControls) {
      if ((percentageControls < 0) || (percentageControls > 50)) {
        return;
      }
      _percentageControls = percentageControls;
    }

    public boolean isGenerateEventFile() {
      return _generateEventFile;
    }
  }

  private class GenericEventIterator implements Iterator<T> {
    private static final int BATCH_SIZE = 100;

    private List<T> currentBatch;
    private int index;

    public GenericEventIterator() {
      initialize();
    }

    @Override
    public boolean hasNext() {
      return true;
    }

    private void initialize() {
      try {
        currentBatch = generateGenericEventList(BATCH_SIZE);
        index = 0;
      } catch (UnknownTypeException | IOException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public T next() {
      if (index < currentBatch.size()) {
        return currentBatch.get(index++);
      } else {
        initialize();
        return next();
      }
    }
  }
}
