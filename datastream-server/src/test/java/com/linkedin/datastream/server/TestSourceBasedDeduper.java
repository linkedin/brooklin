/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.server;

import java.util.Arrays;
import java.util.Collections;
import java.util.Optional;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.linkedin.data.template.StringMap;
import com.linkedin.datastream.common.Datastream;
import com.linkedin.datastream.common.DatastreamDestination;
import com.linkedin.datastream.common.DatastreamMetadataConstants;
import com.linkedin.datastream.common.DatastreamSource;
import com.linkedin.datastream.server.api.connector.DatastreamValidationException;


public class TestSourceBasedDeduper {
  private static final String CONNECTOR_TYPE = "test";

  public static Datastream generateDatastream(int seed, boolean withDestination) {
    Datastream ds = new Datastream();
    ds.setName("name_" + seed);
    ds.setConnectorName(CONNECTOR_TYPE);
    ds.setSource(new DatastreamSource());
    ds.setTransportProviderName("test");
    ds.getSource().setConnectionString("DummySource_" + seed);
    ds.setDestination(new DatastreamDestination());
    if (withDestination) {
      ds.getDestination().setConnectionString("Destination_" + seed);
      ds.getDestination().setPartitions(4);
    }
    StringMap metadata = new StringMap();
    metadata.put("owner", "person_" + seed);
    ds.setMetadata(metadata);
    return ds;
  }

  @Test
  public void testEmptyExistingDatastreams() throws DatastreamValidationException {
    Datastream datastream = generateDatastream(0, false);
    SourceBasedDeduper deduper = new SourceBasedDeduper();
    Assert.assertFalse(deduper.findExistingDatastream(datastream, null).isPresent());
    Assert.assertFalse(deduper.findExistingDatastream(datastream, Collections.emptyList()).isPresent());
  }

  @Test
  public void testDatastreamWithSameSource() throws DatastreamValidationException {
    Datastream datastream = generateDatastream(0, false);
    Datastream datastream1 = generateDatastream(0, true);
    Datastream datastream2 = generateDatastream(1, true);
    SourceBasedDeduper deduper = new SourceBasedDeduper();
    Optional<Datastream> foundDatastream =
        deduper.findExistingDatastream(datastream, Arrays.asList(datastream1, datastream2));
    Assert.assertTrue(foundDatastream.isPresent());
    Assert.assertEquals(foundDatastream.get(), datastream1);
  }

  @Test
  public void testDatastreamWithDifferentSource() throws DatastreamValidationException {
    Datastream datastream = generateDatastream(0, false);
    Datastream datastream1 = generateDatastream(1, true);
    Datastream datastream2 = generateDatastream(2, true);
    SourceBasedDeduper deduper = new SourceBasedDeduper();
    Optional<Datastream> foundDatastream = deduper.findExistingDatastream(datastream, Arrays.asList(datastream1, datastream2));
    Assert.assertFalse(foundDatastream.isPresent());
  }

  @Test
  public void testDatastreamWithSameSourceButNoTopicReuse() throws DatastreamValidationException {
    Datastream datastream = generateDatastream(0, false);
    Datastream datastream1 = generateDatastream(0, true);
    Datastream datastream2 = generateDatastream(1, true);
    datastream1.getMetadata().put(DatastreamMetadataConstants.REUSE_EXISTING_DESTINATION_KEY, "false");
    SourceBasedDeduper deduper = new SourceBasedDeduper();
    Optional<Datastream> foundDatastream = deduper.findExistingDatastream(datastream, Arrays.asList(datastream1, datastream2));
    Assert.assertFalse(foundDatastream.isPresent());
  }

  @Test
  public void testDifferentDatastreamsWithSameSource() throws DatastreamValidationException {
    Datastream datastream = generateDatastream(0, false);
    Datastream datastream1 = generateDatastream(0, true);
    Datastream datastream2 = generateDatastream(1, true);
    SourceBasedDeduper deduper = new SourceBasedDeduper();
    Assert.assertTrue(deduper.findExistingDatastream(datastream, Arrays.asList(datastream1, datastream2)).isPresent());

    datastream1.setTransportProviderName("foo");
    Assert.assertFalse(deduper.findExistingDatastream(datastream, Arrays.asList(datastream1, datastream2)).isPresent());

    datastream1.setTransportProviderName(datastream.getTransportProviderName());
    Assert.assertTrue(deduper.findExistingDatastream(datastream, Arrays.asList(datastream1, datastream2)).isPresent());

    datastream1.getDestination().setKeySerDe("serde");
    Assert.assertFalse(deduper.findExistingDatastream(datastream, Arrays.asList(datastream1, datastream2)).isPresent());

    datastream.setDestination(new DatastreamDestination());
    datastream.getDestination().setKeySerDe(datastream1.getDestination().getKeySerDe());
    Assert.assertTrue(deduper.findExistingDatastream(datastream, Arrays.asList(datastream1, datastream2)).isPresent());

    datastream1.getDestination().setPayloadSerDe("serde");
    Assert.assertFalse(deduper.findExistingDatastream(datastream, Arrays.asList(datastream1, datastream2)).isPresent());

    datastream.getDestination().setPayloadSerDe(datastream1.getDestination().getPayloadSerDe());
    Assert.assertTrue(deduper.findExistingDatastream(datastream, Arrays.asList(datastream1, datastream2)).isPresent());

    datastream1.getDestination().setEnvelopeSerDe("serde");
    Assert.assertFalse(deduper.findExistingDatastream(datastream, Arrays.asList(datastream1, datastream2)).isPresent());

    datastream.getDestination().setEnvelopeSerDe(datastream1.getDestination().getEnvelopeSerDe());
    Assert.assertTrue(deduper.findExistingDatastream(datastream, Arrays.asList(datastream1, datastream2)).isPresent());
  }
}
