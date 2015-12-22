package com.linkedin.datastream.testutil;

import com.linkedin.datastream.common.Datastream;
import com.linkedin.datastream.common.DatastreamEvent;

import com.linkedin.datastream.testutil.event.generator.DatastreamEventGenerator;
import java.lang.CharSequence;
import java.lang.Object;
import java.lang.Override;
import java.lang.String;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;


public class TestEventGenerator {
  private static Logger LOG = LoggerFactory.getLogger(TestEventGenerator.class);

  @BeforeMethod
  public void setup() throws IOException {
    LOG.info("Test EventGenerator class");
  }

  @AfterMethod
  public void teardown() {
    LOG.info("Done testing EventGenerator class");
  }

  private int _numEvents = 10;
  private int _percentageUpdates = 20;
  private int _percentageDeletes = 20;

  @Test
  public void testDatastreamEvents() throws Exception {
    // set up
    String schema =
        "{" + "   \"name\" : \"TestSchema\"," + "   \"version\": 1," + "   \"type\" : \"record\","
            + "   \"namespace\" : \"com.linkedin.datastream.test.TestDatastreamEvents\"," + "   \"fields\" : [ "
            + "   {" + "     \"name\" : \"company\"," + "     \"type\" : \"string\"" + "   }, {"
            + "     \"name\" : \"ceo\"," + "     \"type\" : \"string\"" + "   }, {" + "     \"name\" : \"taxId\","
            + "     \"type\" : \"long\"" + "   } ]" + " }";

    DatastreamEventGenerator eeg = new DatastreamEventGenerator(schema);
    eeg.setPercentageUpdates(_percentageUpdates);
    eeg.setPercentageDeletes(_percentageDeletes);

    // generate events
    List<Object> eventList = eeg.generateGenericEventList(_numEvents); // todo this should be generateEventList

    // verify the events
    int numUpdates = 0;
    int numDeletes = 0;
    Assert.assertEquals(_numEvents, eventList.size());
    for (Object obj : eventList) {
      @SuppressWarnings("unchecked")
      // just map
      DatastreamEvent espEvent = (DatastreamEvent) obj;
      CharSequence op = espEvent.metadata.get("__OpCode");
      if (op.equals("UPDATE")) {
        ++numUpdates;
      } else if (op.equals("DELETE")) {
        ++numDeletes;
      }
    }
    Assert.assertEquals(numUpdates, _numEvents * _percentageUpdates / 100);
    Assert.assertEquals(numDeletes, _numEvents * _percentageDeletes / 100);
  }
}
