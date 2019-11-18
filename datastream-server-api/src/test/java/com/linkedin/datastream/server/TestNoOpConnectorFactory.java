/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.server;

import java.util.Arrays;
import java.util.Properties;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.linkedin.data.template.StringMap;
import com.linkedin.datastream.common.Datastream;
import com.linkedin.datastream.common.DatastreamDestination;
import com.linkedin.datastream.common.DatastreamSource;
import com.linkedin.datastream.server.api.connector.DatastreamValidationException;

/**
 * Tests for {@link NoOpConnectorFactory}
 */
public class TestNoOpConnectorFactory {

  private static Datastream createDatastream(String datastreamName, String transportProviderName) {
    // create the datastream
    Datastream datastream = new Datastream();
    datastream.setName(datastreamName);
    datastream.setConnectorName("noOpConnector");
    DatastreamSource source = new DatastreamSource();
    source.setConnectionString("source_connection_string");
    datastream.setSource(source);
    DatastreamDestination destination = new DatastreamDestination();
    destination.setConnectionString("ListBackedTransportProvider");
    datastream.setDestination(destination);
    datastream.setTransportProviderName(transportProviderName);
    datastream.setMetadata(new StringMap());
    return datastream;
  }

  @Test
  public void testvalidateAndUpdateDatastreams() throws Exception {
    Properties properties = new Properties();

    NoOpConnectorFactory noOpConnectorFactory = new NoOpConnectorFactory();
    NoOpConnectorFactory.NoOpConnector connector =
        noOpConnectorFactory.createConnector("noOpConnector", properties, "cluster");

    Datastream datastream1 = createDatastream("noOp-1", "likafkakac");
    Datastream datastream2 = createDatastream("noOp-2", "likafkakac");

    // Happy path - no change in either datastream
    connector.validateAndUpdateDatastreams(Arrays.asList(datastream1, datastream2), Arrays.asList(datastream1, datastream2));

    // Try to modify source on one of the datastreams
    Datastream datastream3 = createDatastream("noOp-2", "likafkakac");
    datastream3.getSource().setConnectionString("source-blah");
    try {
      connector.validateAndUpdateDatastreams(Arrays.asList(datastream1, datastream3), Arrays.asList(datastream1, datastream2));
      Assert.fail();
    } catch (DatastreamValidationException e) {
    }

    // Try to modify destination on one of the datastreams
    datastream3 = createDatastream("noOp-1", "likafkakac");
    datastream3.getDestination().setConnectionString("dest-blah");
    try {
      connector.validateAndUpdateDatastreams(Arrays.asList(datastream3, datastream2), Arrays.asList(datastream1, datastream2));
      Assert.fail();
    } catch (DatastreamValidationException e) {
    }

    // Try to modify the transport provider name on one of the datastreams
    datastream3 = createDatastream("noOp-2", "likafkakactest");
    try {
      connector.validateAndUpdateDatastreams(Arrays.asList(datastream1, datastream3), Arrays.asList(datastream1, datastream2));
      Assert.fail();
    } catch (DatastreamValidationException e) {
    }

    // Happy path - try to modify the metadata on one of the datastreams
    datastream3 = createDatastream("noOp-1", "likafkakac");
    datastream3.getMetadata().put("random-metadata-key", "random-metadata-value");
    connector.validateAndUpdateDatastreams(Arrays.asList(datastream3, datastream2), Arrays.asList(datastream1, datastream2));

    // Happy path - modify already existing metadata on one of the datastreams
    datastream2.getMetadata().put("metadata-key", "metadata-value");
    datastream3 = createDatastream("noOp-2", "likafkakac");
    datastream3.getMetadata().put("metadata-key", "new-metadata-value");
    connector.validateAndUpdateDatastreams(Arrays.asList(datastream1, datastream3), Arrays.asList(datastream1, datastream2));
  }
}
