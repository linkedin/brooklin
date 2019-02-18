/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.testutil;

import com.linkedin.datastream.common.DatastreamEvent;
import com.linkedin.datastream.testutil.event.generator.AbstractEventGenerator;
import com.linkedin.datastream.testutil.event.generator.DatastreamEventGenerator;
import com.linkedin.datastream.testutil.event.validator.GenericEventValidator;
import java.io.IOException;
import java.util.List;

import org.apache.avro.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class TestEventGenerator {
  private static final Logger LOG = LoggerFactory.getLogger(TestEventGenerator.class);

  @BeforeMethod
  public void setup()
      throws IOException {
    LOG.info("Test EventGenerator class");
  }

  @AfterMethod
  public void teardown() {
    LOG.info("Done testing EventGenerator class");
  }

  @Test
  public void testDatastreamEvents()
      throws Exception {
    int numEvents = 10;
    int percentageUpdates = 20;
    int percentageDeletes = 20;
    // set up
    String schema = "{" + "   \"name\" : \"TestSchema\"," + "   \"version\": 1," + "   \"type\" : \"record\","
        + "   \"namespace\" : \"com.linkedin.datastream.test.TestDatastreamEvents\"," + "   \"fields\" : [ " + "   {"
        + "     \"name\" : \"company\"," + "     \"type\" : \"string\"" + "   }, {" + "     \"name\" : \"ceo\","
        + "     \"type\" : \"string\"" + "   }, {" + "     \"name\" : \"taxId\"," + "     \"type\" : \"long\""
        + "   } ]" + " }";

    AbstractEventGenerator.EventGeneratorConfig generatorConfig = new AbstractEventGenerator.EventGeneratorConfig();
    generatorConfig.setPercentageUpdates(percentageUpdates);
    generatorConfig.setPercentageDeletes(percentageDeletes);
    DatastreamEventGenerator eeg = new DatastreamEventGenerator(Schema.parse(schema), generatorConfig);

    // generate events
    List<DatastreamEvent> eventList = eeg.generateGenericEventList(numEvents); // todo this should be generateEventList
    List<DatastreamEvent> eventList2 = eeg.generateGenericEventList(numEvents);

    Assert.assertTrue(GenericEventValidator.validateGenericEventList(eventList, eventList),
        "Same event lists should pass the event validation");
    Assert.assertFalse(GenericEventValidator.validateGenericEventList(eventList, eventList2),
        "Different event lists should fail the event validation");

    // verify the events
    int numUpdates = 0;
    int numDeletes = 0;
    Assert.assertEquals(numEvents, eventList.size());
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
    Assert.assertEquals(numUpdates, numEvents * percentageUpdates / 100);
    Assert.assertEquals(numDeletes, numEvents * percentageDeletes / 100);
  }
}
