package com.linkedin.datastream.server;

import java.time.Duration;
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
import com.linkedin.datastream.server.api.connector.SourceBasedDeduper;


public class TestSourceBasedDeduper {

  private static final Duration RETENTION = Duration.ofDays(3);
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
    }
    StringMap metadata = new StringMap();
    metadata.put("owner", "person_" + seed);
    ds.setMetadata(metadata);
    return ds;
  }

  @Test
  public void testEmptyExistingDatastreams() {
    Datastream datastream = generateDatastream(0, false);
    SourceBasedDeduper deduper = new SourceBasedDeduper();
    Assert.assertFalse(deduper.findExistingDatastream(datastream, Collections.emptyList()).isPresent());

    try {
      deduper.findExistingDatastream(null, Collections.emptyList());
      Assert.fail("Expected exception");
    } catch (IllegalArgumentException e) {
    }

    try {
      deduper.findExistingDatastream(datastream, null);
      Assert.fail("Expected exception");
    } catch (IllegalArgumentException e) {
    }
  }

  @Test
  public void testDatastreamWithSameSource() {
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
  public void testDatastreamWithDifferentSource() {
    Datastream datastream = generateDatastream(0, false);
    Datastream datastream1 = generateDatastream(1, true);
    Datastream datastream2 = generateDatastream(2, true);
    SourceBasedDeduper deduper = new SourceBasedDeduper();
    Optional<Datastream> foundDatastream =
        deduper.findExistingDatastream(datastream, Arrays.asList(datastream1, datastream2));
    Assert.assertFalse(foundDatastream.isPresent());
  }

  @Test
  public void testDatastreamWithSameSourceButNoTopicReuse() {
    Datastream datastream = generateDatastream(0, false);
    Datastream datastream1 = generateDatastream(0, true);
    Datastream datastream2 = generateDatastream(1, true);
    datastream1.getMetadata().put(DatastreamMetadataConstants.REUSE_EXISTING_DESTINATION_KEY, "false");
    SourceBasedDeduper deduper = new SourceBasedDeduper();
    Optional<Datastream> foundDatastream =
        deduper.findExistingDatastream(datastream, Arrays.asList(datastream1, datastream2));
    Assert.assertFalse(foundDatastream.isPresent());
  }

  @Test
  public void testDifferentDatastreamsWithSameSource() {
    Datastream datastream = generateDatastream(0, false);
    Datastream datastream1 = generateDatastream(0, true);
    Datastream datastream2 = generateDatastream(1, true);
    datastream1.setConnectorName("foo");
    SourceBasedDeduper deduper = new SourceBasedDeduper();
    Optional<Datastream> foundDatastream =
        deduper.findExistingDatastream(datastream, Arrays.asList(datastream1, datastream2));
    Assert.assertFalse(foundDatastream.isPresent());

    datastream1.setConnectorName(datastream.getConnectorName());
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
