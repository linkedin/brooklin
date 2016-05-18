package com.linkedin.datastream.testutil.event.generator;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.util.List;

import org.apache.avro.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.datastream.common.DatastreamEvent;


public class DatastreamEventProducerRunnable implements Runnable {
  private static final Logger LOG = LoggerFactory.getLogger(DatastreamEventProducerRunnable.class.getName());
  private final GlobalSettings _globalSettings;

  /**
   * This producer basically ignores the type of request for now
   * It will produce
   * @param globalSettings
   */
  public DatastreamEventProducerRunnable(GlobalSettings globalSettings) {
    _globalSettings = globalSettings;

  }

  /**
   */
  @Override
  public void run() {
    File schemaFile = new File(_globalSettings._schemataPath, _globalSettings._schemaFileName);

    AbstractEventGenerator.EventGeneratorConfig generatorConfig = new AbstractEventGenerator.EventGeneratorConfig();
    generatorConfig.setDbName(_globalSettings._dbName);
    generatorConfig.setTableName(_globalSettings._tableName);
    generatorConfig.setStartScn(_globalSettings._startResourceKey);
    generatorConfig.setNumPartitions(_globalSettings._numPartitions);
    generatorConfig.setMaxTransactionSize(_globalSettings._maxTransactionSize);

    String[] dataPerc = _globalSettings._percentData.split(",");

    if (dataPerc.length > 1) {
      generatorConfig.setPercentageUpdates(Integer.valueOf(dataPerc[1]));
    }
    if (dataPerc.length > 2) {
      generatorConfig.setPercentageDeletes(Integer.valueOf(dataPerc[2]));
    }
    if (dataPerc.length > 3) {
      generatorConfig.setPercentageControls(Integer.valueOf(dataPerc[3]));
    }

    DatastreamEventGenerator deg = null;
    try {
      deg = new DatastreamEventGenerator(Schema.parse(schemaFile), generatorConfig);
    } catch (java.io.IOException exp) {
      LOG.error("Got IOException " + exp.getStackTrace());
    }

    if (deg == null) { // can't handle any better right now
      return;
    }
    deg.setDatastreamName(_globalSettings._datastreamName);


    // generate events
    // List<Object> eventList = null;
    try {
      List<DatastreamEvent> eventList = deg.generateGenericEventList(_globalSettings._numEvents); // todo this should be posting to the proper producer
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
