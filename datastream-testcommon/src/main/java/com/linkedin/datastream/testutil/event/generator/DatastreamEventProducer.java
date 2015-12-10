package com.linkedin.datastream.testutil.event.generator;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.util.List;
import com.linkedin.datastream.common.DatastreamEvent;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class DatastreamEventProducer implements Runnable {
  private static final Logger LOG = LoggerFactory.getLogger(DatastreamEventProducer.class.getName());
  private final GlobalSettings _globalSettings;

  /**
   * This producer basically ignores the type of request for now
   * It will produce
   * @param globalSettings
   */
  public DatastreamEventProducer(GlobalSettings globalSettings) {
    _globalSettings = globalSettings;

  }

  /**
   */
  @Override
  public void run() {
    File schemaFile = new File(_globalSettings._schemataPath, _globalSettings._schemaFileName);
    DatastreamEventGenerator deg = null;
    try {
      deg = new DatastreamEventGenerator(schemaFile);
    } catch (java.io.IOException exp) {
      LOG.error("Got IOException " + exp.getStackTrace());
    }

    if (deg == null) { // can't handle any better right now
      return;
    }
    deg.setDatastreamName(_globalSettings._datastreamName);
    deg.setDbName(_globalSettings._dbName);
    deg.setTableName(_globalSettings._tableName);
    deg.setStartScn(_globalSettings._startResourceKey);
    deg.setNumPartitions(_globalSettings._numPartitions);
    deg.setMaxTransactionSize(_globalSettings._maxTransactionSize);

    String[] dataPerc = _globalSettings._percentData.split(",");

    if (dataPerc.length > 1) {
      deg.setPercentageUpdates(Integer.valueOf(dataPerc[1]));
    }
    if (dataPerc.length > 2) {
      deg.setPercentageDeletes(Integer.valueOf(dataPerc[2]));
    }
    if (dataPerc.length > 3) {
      deg.setPercentageControls(Integer.valueOf(dataPerc[3]));
    }

    // generate events
    // List<Object> eventList = null;
    try {
      List<Object> eventList = deg.generateGenericEventList(_globalSettings._numEvents); // todo this should be posting to the proper producer
      BufferedWriter indexWriter = new BufferedWriter(new FileWriter(_globalSettings._dataFileName));
      for (Object obj : eventList) {
        @SuppressWarnings("unchecked")
        // just map
        DatastreamEvent dsEvent = (DatastreamEvent) obj;
        indexWriter.write(dsEvent.toString());
        indexWriter.newLine();
      }
      indexWriter.flush();
      indexWriter.close();
    } catch (UnknownTypeException exp) {
      LOG.error("Unknown schema field Exception " + exp.getStackTrace());
    } catch (java.io.IOException exp) {
      LOG.error("Unknown schema field Exception " + exp.getStackTrace());
    }
  }

}
