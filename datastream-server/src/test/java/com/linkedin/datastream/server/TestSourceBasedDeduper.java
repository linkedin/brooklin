/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.server;

import java.util.Arrays;
import java.util.Collections;
import java.util.Optional;
import java.util.Properties;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.linkedin.data.template.StringMap;
import com.linkedin.datastream.common.Datastream;
import com.linkedin.datastream.common.DatastreamDestination;
import com.linkedin.datastream.common.DatastreamMetadataConstants;
import com.linkedin.datastream.common.DatastreamSource;
import com.linkedin.datastream.server.api.connector.DatastreamValidationException;


/**
 * Tests for {@link SourceBasedDeduper}
 */
public class TestSourceBasedDeduper {
  private static final String CONNECTOR_TYPE = "test";

  /**
   * Generate a datastream
   */
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

  /**
   * Generate a datastream decorated with CDC-bootstrap metadata and an optional creation timestamp.
   * {@code creationMs = 0} simulates an old stream whose time window has expired;
   * {@code creationMs = System.currentTimeMillis()} simulates a freshly-created stream still inside its window.
   * Pass {@code creationMs < 0} to omit the {@code system.creation.ms} key entirely.
   */
  private static Datastream generateCdcDatastream(int seed, boolean withDestination,
      boolean cdcBootstrapRequired, long creationMs) {
    Datastream ds = generateDatastream(seed, withDestination);
    ds.getMetadata().put(SourceBasedDeduper.DEFAULT_CDC_BOOTSTRAP_REQUIRED_METADATA_KEY,
        String.valueOf(cdcBootstrapRequired));
    if (creationMs >= 0) {
      ds.getMetadata().put(DatastreamMetadataConstants.CREATION_MS, String.valueOf(creationMs));
    }
    return ds;
  }

  private static SourceBasedDeduper cdcDeduper() {
    Properties props = new Properties();
    props.setProperty(SourceBasedDeduper.CONFIG_NEW_STREAM_GRACE_PERIOD_MS, "3600000"); // 1 hour
    props.setProperty(SourceBasedDeduper.CONFIG_SNAPSHOT_WORST_REFRESH_DAYS, "4");
    return new SourceBasedDeduper(props);
  }

  // ---- Case 1: new CDC+BST → existing CDC+BST → always dedup ----
  @Test
  public void testCdcBstNewVsExistingCdcBst() throws DatastreamValidationException {
    Datastream newStream = generateCdcDatastream(0, false, true, -1);
    Datastream candidate = generateCdcDatastream(0, true, true, System.currentTimeMillis());
    Optional<Datastream> result = cdcDeduper().findExistingDatastream(newStream, Collections.singletonList(candidate));
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals(result.get(), candidate);
  }

  // ---- Case 2: new CDC-only + existing CDC+BST whose grace window is ACTIVE → new destination ----
  @Test
  public void testCdcOnlyNewVsExistingCdcBstGraceActive() throws DatastreamValidationException {
    Datastream newStream = generateCdcDatastream(0, false, false, -1);
    // creationMs = now → grace window (1 hour) has not expired
    Datastream candidate = generateCdcDatastream(0, true, true, System.currentTimeMillis());
    Optional<Datastream> result = cdcDeduper().findExistingDatastream(newStream, Collections.singletonList(candidate));
    Assert.assertFalse(result.isPresent());
  }

  // ---- Case 3: new CDC-only + existing CDC+BST whose grace window has EXPIRED → dedup ----
  @Test
  public void testCdcOnlyNewVsExistingCdcBstGraceExpired() throws DatastreamValidationException {
    Datastream newStream = generateCdcDatastream(0, false, false, -1);
    // creationMs = 0 (epoch) → grace window always expired
    Datastream candidate = generateCdcDatastream(0, true, true, 0);
    Optional<Datastream> result = cdcDeduper().findExistingDatastream(newStream, Collections.singletonList(candidate));
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals(result.get(), candidate);
  }

  // ---- Case 4: new CDC+BST + existing CDC-only whose snapshot-refresh window is ACTIVE → new destination ----
  @Test
  public void testCdcBstNewVsExistingCdcOnlySnapshotWindowActive() throws DatastreamValidationException {
    Datastream newStream = generateCdcDatastream(0, false, true, -1);
    // creationMs = now → 4-day snapshot window has not expired
    Datastream candidate = generateCdcDatastream(0, true, false, System.currentTimeMillis());
    Optional<Datastream> result = cdcDeduper().findExistingDatastream(newStream, Collections.singletonList(candidate));
    Assert.assertFalse(result.isPresent());
  }

  // ---- Case 5: new CDC+BST + existing CDC-only whose snapshot-refresh window has EXPIRED → dedup ----
  @Test
  public void testCdcBstNewVsExistingCdcOnlySnapshotWindowExpired() throws DatastreamValidationException {
    Datastream newStream = generateCdcDatastream(0, false, true, -1);
    // creationMs = 0 (epoch) → 4-day snapshot window always expired
    Datastream candidate = generateCdcDatastream(0, true, false, 0);
    Optional<Datastream> result = cdcDeduper().findExistingDatastream(newStream, Collections.singletonList(candidate));
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals(result.get(), candidate);
  }

  // ---- Case 6: new CDC-only + existing CDC-only → always dedup (no timing check) ----
  @Test
  public void testCdcOnlyNewVsExistingCdcOnly() throws DatastreamValidationException {
    Datastream newStream = generateCdcDatastream(0, false, false, -1);
    Datastream candidate = generateCdcDatastream(0, true, false, System.currentTimeMillis());
    Optional<Datastream> result = cdcDeduper().findExistingDatastream(newStream, Collections.singletonList(candidate));
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals(result.get(), candidate);
  }

  // ---- Case 7: legacy path — new stream has no system.cdcBootstrapRequired → source-equality match ----
  @Test
  public void testLegacyPathNoCdcBootstrapKey() throws DatastreamValidationException {
    // New stream: no system.cdcBootstrapRequired key at all (uses generateDatastream, not generateCdcDatastream)
    Datastream newStream = generateDatastream(0, false);
    // Candidate carries the metadata key but that should not matter for the legacy path
    Datastream candidateSameSource = generateCdcDatastream(0, true, true, 0);
    Datastream candidateDifferentSource = generateCdcDatastream(1, true, true, 0);

    SourceBasedDeduper deduper = cdcDeduper();
    // Same source → dedup (legacy source-equality logic)
    Optional<Datastream> result = deduper.findExistingDatastream(newStream,
        Arrays.asList(candidateSameSource, candidateDifferentSource));
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals(result.get(), candidateSameSource);

    // Different source → no match
    Assert.assertFalse(deduper.findExistingDatastream(newStream,
        Collections.singletonList(candidateDifferentSource)).isPresent());
  }

  // ---- Preference order: CDC+BST candidate takes priority over CDC-only candidate for new CDC+BST stream ----
  @Test
  public void testCdcBstNewPrefersExistingCdcBstOverExpiredCdcOnly() throws DatastreamValidationException {
    Datastream newStream = generateCdcDatastream(0, false, true, -1);
    Datastream cdcOnly = generateCdcDatastream(0, true, false, 0); // snapshot window expired
    Datastream cdcBst = generateCdcDatastream(0, true, true, System.currentTimeMillis());
    Optional<Datastream> result = cdcDeduper().findExistingDatastream(newStream, Arrays.asList(cdcOnly, cdcBst));
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals(result.get(), cdcBst);
  }

  // ---- Preference order: new CDC-only prefers expired CDC+BST over CDC-only when both are present ----
  @Test
  public void testCdcOnlyNewPrefersExpiredCdcBstOverCdcOnly() throws DatastreamValidationException {
    Datastream newStream = generateCdcDatastream(0, false, false, -1);
    Datastream cdcBst = generateCdcDatastream(0, true, true, 0); // grace expired
    Datastream cdcOnly = generateCdcDatastream(0, true, false, System.currentTimeMillis());
    Optional<Datastream> result = cdcDeduper().findExistingDatastream(newStream, Arrays.asList(cdcBst, cdcOnly));
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals(result.get(), cdcBst);
  }

  // ---- hasWindowExpired: absent system.creation.ms → conservatively treated as NOT expired ----

  // New CDC-only + CDC+BST candidate with no creation.ms → window cannot be verified → new destination
  @Test
  public void testCdcOnlyNewVsCdcBstMissingCreationMs() throws DatastreamValidationException {
    Datastream newStream = generateCdcDatastream(0, false, false, -1);
    // creationMs < 0 → no system.creation.ms key set on candidate
    Datastream candidate = generateCdcDatastream(0, true, true, -1);
    Optional<Datastream> result = cdcDeduper().findExistingDatastream(newStream, Collections.singletonList(candidate));
    Assert.assertFalse(result.isPresent());
  }

  // New CDC+BST + CDC-only candidate with no creation.ms → window cannot be verified → new destination
  @Test
  public void testCdcBstNewVsCdcOnlyMissingCreationMs() throws DatastreamValidationException {
    Datastream newStream = generateCdcDatastream(0, false, true, -1);
    // No system.creation.ms → hasWindowExpired returns false → candidate not eligible
    Datastream candidate = generateCdcDatastream(0, true, false, -1);
    Optional<Datastream> result = cdcDeduper().findExistingDatastream(newStream, Collections.singletonList(candidate));
    Assert.assertFalse(result.isPresent());
  }

  // ---- Default deduper (windowMs=0): all candidates with any creation.ms are immediately eligible ----
  @Test
  public void testDefaultDedupeConfigWindowsDisabled() throws DatastreamValidationException {
    // Default constructor → both windows default to 0 → hasWindowExpired always true for any stream with creation.ms
    SourceBasedDeduper defaultDeduper = new SourceBasedDeduper();

    // New CDC-only vs CDC+BST with very recent creation.ms: window=0 → immediately expired → dedup
    Datastream newCdcOnly = generateCdcDatastream(0, false, false, -1);
    Datastream cdcBstCandidate = generateCdcDatastream(0, true, true, System.currentTimeMillis());
    Optional<Datastream> result = defaultDeduper.findExistingDatastream(newCdcOnly,
        Collections.singletonList(cdcBstCandidate));
    Assert.assertTrue(result.isPresent());

    // New CDC+BST vs CDC-only with very recent creation.ms: window=0 → immediately expired → dedup
    Datastream newCdcBst = generateCdcDatastream(0, false, true, -1);
    Datastream cdcOnlyCandidate = generateCdcDatastream(0, true, false, System.currentTimeMillis());
    result = defaultDeduper.findExistingDatastream(newCdcBst, Collections.singletonList(cdcOnlyCandidate));
    Assert.assertTrue(result.isPresent());
  }

  // ---- Different source in CDC-aware path → no match ----
  @Test
  public void testCdcAwarePathDifferentSourceNoMatch() throws DatastreamValidationException {
    Datastream newStream = generateCdcDatastream(0, false, true, -1);
    // Candidate has different source (seed=1)
    Datastream differentSourceCandidate = generateCdcDatastream(1, true, true, System.currentTimeMillis());
    Datastream differentSourceCdcOnly = generateCdcDatastream(1, true, false, 0);
    Assert.assertFalse(cdcDeduper().findExistingDatastream(newStream,
        Arrays.asList(differentSourceCandidate, differentSourceCdcOnly)).isPresent());
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
