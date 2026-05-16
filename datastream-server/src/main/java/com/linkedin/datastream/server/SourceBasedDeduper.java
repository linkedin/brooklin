/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.server;

import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.datastream.common.Datastream;
import com.linkedin.datastream.common.DatastreamMetadataConstants;
import com.linkedin.datastream.server.api.connector.DatastreamValidationException;


/**
 * Deduper that uses the source connection string to determine whether two datastreams can share
 * the same destination and tasks.
 *
 * <h3>Legacy behavior (default)</h3>
 * <p>When the new stream does not carry the CDC-bootstrap metadata key (configurable via
 * {@link #CONFIG_CDC_BOOTSTRAP_REQUIRED_METADATA_KEY},
 * default {@value #DEFAULT_CDC_BOOTSTRAP_REQUIRED_METADATA_KEY}), the original logic applies:
 * any existing stream with an identical source connection string is a dedup candidate and the
 * first match is returned.
 *
 * <h3>CDC-bootstrap-aware behavior</h3>
 * <p>When {@value #DEFAULT_CDC_BOOTSTRAP_REQUIRED_METADATA_KEY} is present on the new stream,
 * CDC-bootstrap-aware dedup is activated. Two stream types are recognized:
 * <ul>
 *   <li><em>CDC-only</em>  — metadata key absent or set to {@code false}. The consumer offset
 *       is initialized to the <em>latest</em> Kafka offset, so the stream only reads events
 *       produced after it was created.</li>
 *   <li><em>CDC+BST</em>   — metadata key set to {@code true}. The consumer offset of source topic is
 *       intentionally initialized to an <em>earlier</em> offset (before the current tail) so
 *       that the stream can catch up on historical lag before switching to live replication.
 *       During this catch-up window the stream is considered "in grace period".</li>
 * </ul>
 *
 * <p>Cross-type dedup eligibility is governed by two configurable time windows. Both default
 * to {@code 0}, which means the window is treated as immediately expired and cross-type dedup
 * is always permitted when no same-type candidate is found.
 *
 * <ul>
 *   <li>{@link #CONFIG_NEW_STREAM_GRACE_PERIOD_MS} — the grace period (in milliseconds) that a
 *       CDC+BST stream requires to finish catching up on lag from its earlier start offset.
 *       A new CDC-only stream arriving during this window cannot dedup against the CDC+BST stream
 *       because the CDC+BST stream's offsets are behind the live tail; the CDC-only stream would
 *       miss events that the CDC+BST has not yet replicated. Once the grace window expires, the
 *       CDC+BST stream has caught up and cross-type dedup is safe.</li>
 *   <li>{@link #CONFIG_SNAPSHOT_WORST_REFRESH_DAYS} — the worst-case snapshot refresh window
 *       (in days) for a CDC-only stream. When a new CDC+BST stream arrives, it needs to start
 *       from an earlier offset to capture a full snapshot. If the existing CDC-only stream was
 *       created within this window, its starting offset is still recent enough that the CDC+BST
 *       stream's required earlier offset would fall outside what the CDC-only stream covers;
 *       a new independent destination must be created. Once this window has expired, the CDC-only
 *       stream is old enough that deduping the new CDC+BST stream against it is safe.</li>
 * </ul>
 */
public class SourceBasedDeduper extends AbstractDatastreamDeduper {
  private static final Logger LOG = LoggerFactory.getLogger(SourceBasedDeduper.class);

  /** Config key for the metadata flag that marks a stream as requiring CDC bootstrap. */
  public static final String CONFIG_CDC_BOOTSTRAP_REQUIRED_METADATA_KEY = "cdcBootstrapRequiredMetadataKey";

  /** Default value of the metadata key used to identify CDC+BST streams. */
  public static final String DEFAULT_CDC_BOOTSTRAP_REQUIRED_METADATA_KEY = "system.cdcBootstrapRequired";

  /**
   * Grace-period window in <em>milliseconds</em> applied to existing CDC+BST streams when a new
   * CDC-only stream is being created. A CDC+BST candidate is eligible for dedup only after this
   * window has expired. Default {@code 0} means the window is treated as immediately expired
   * (always eligible).
   *
   * <p>This key intentionally shares the same unit and semantics as
   * {@code brooklin.server.eventProducer.newStreamGracePeriodMs} so that a single config value
   * can drive both the EventProducer SLA grace period and this dedup eligibility window.
   */
  public static final String CONFIG_NEW_STREAM_GRACE_PERIOD_MS = "newStreamGracePeriodMs";

  /**
   * Snapshot-refresh window in days applied to existing CDC-only streams when a new CDC+BST stream
   * is being created. A CDC-only candidate is eligible for dedup only after this window has expired.
   * Default {@code 0} means the window is treated as immediately expired (always eligible).
   */
  public static final String CONFIG_SNAPSHOT_WORST_REFRESH_DAYS = "snapshotWorstRefreshDays";

  private static final long DEFAULT_NEW_STREAM_GRACE_PERIOD_MS = 0;
  private static final long DEFAULT_SNAPSHOT_WORST_REFRESH_DAYS = 0;

  private static final long MS_PER_DAY = 24 * 3_600_000L;

  private final String _cdcBootstrapRequiredMetadataKey;
  private final long _newStreamGracePeriodMs;
  private final long _snapshotWorstRefreshMs;

  /** Creates a deduper with all settings at their defaults. */
  public SourceBasedDeduper() {
    this(new Properties());
  }

  /**
   * Creates a deduper configured from {@code props}.
   *
   * @param props deduper-domain properties (keys without the {@code deduper.} prefix,
   *              as produced by {@code VerifiableProperties.getDomainProperties("deduper")})
   */
  public SourceBasedDeduper(Properties props) {
    _cdcBootstrapRequiredMetadataKey = props.getProperty(CONFIG_CDC_BOOTSTRAP_REQUIRED_METADATA_KEY,
        DEFAULT_CDC_BOOTSTRAP_REQUIRED_METADATA_KEY);
    _newStreamGracePeriodMs = Long.parseLong(props.getProperty(CONFIG_NEW_STREAM_GRACE_PERIOD_MS,
        String.valueOf(DEFAULT_NEW_STREAM_GRACE_PERIOD_MS)));
    _snapshotWorstRefreshMs = Long.parseLong(props.getProperty(CONFIG_SNAPSHOT_WORST_REFRESH_DAYS,
        String.valueOf(DEFAULT_SNAPSHOT_WORST_REFRESH_DAYS))) * MS_PER_DAY;
  }

  /**
   * Entry point. Delegates to legacy source-equality logic when the new stream has no
   * CDC-bootstrap metadata key; otherwise delegates to CDC-bootstrap-aware logic.
   */
  @Override
  public Optional<Datastream> dedupeStreams(Datastream stream, List<Datastream> candidates)
      throws DatastreamValidationException {
    if (!stream.hasMetadata() || stream.getMetadata() == null
        || !stream.getMetadata().containsKey(_cdcBootstrapRequiredMetadataKey)) {
      return findDedupCandidateBySource(stream, candidates);
    }
    return findDedupCandidateCdcBootstrapAware(stream, candidates);
  }

  /**
   * Legacy dedup: returns the first existing stream whose source connection string matches the
   * new stream's source. Used for all non-CDC connectors and any stream lacking the bootstrap flag.
   *
   * @param newStream  the stream being created
   * @param candidates pre-filtered existing streams that meet the baseline reuse requirements
   * @return the first matching stream, or {@link Optional#empty()} if none found
   */
  private Optional<Datastream> findDedupCandidateBySource(Datastream newStream, List<Datastream> candidates) {
    List<Datastream> sameSourceStreams = candidates.stream()
        .filter(d -> d.getSource().equals(newStream.getSource()))
        .collect(Collectors.toList());

    if (sameSourceStreams.isEmpty()) {
      return Optional.empty();
    }
    LOG.info("Deduping stream {} against existing stream {} (source match)",
        newStream.getName(), sameSourceStreams.get(0).getName());
    return Optional.of(sameSourceStreams.get(0));
  }

  /**
   * CDC-bootstrap-aware dedup. Splits same-source candidates into CDC-only and CDC+BST buckets,
   * then delegates based on whether the new stream itself requires bootstrap.
   *
   * @param newStream  the stream being created (must carry the CDC-bootstrap metadata key)
   * @param candidates pre-filtered existing streams that meet the baseline reuse requirements
   * @return the chosen dedup target, or {@link Optional#empty()} if a new destination should be created
   */
  private Optional<Datastream> findDedupCandidateCdcBootstrapAware(Datastream newStream,
      List<Datastream> candidates) {
    boolean newStreamRequiresCdcBootstrap = Boolean.parseBoolean(
        newStream.getMetadata().getOrDefault(_cdcBootstrapRequiredMetadataKey, "false"));

    List<Datastream> sameSourceCandidates = candidates.stream()
        .filter(d -> d.getSource().equals(newStream.getSource()))
        .collect(Collectors.toList());

    List<Datastream> cdcOnlyCandidates = sameSourceCandidates.stream()
        .filter(d -> !isCdcBootstrapRequiredStream(d))
        .collect(Collectors.toList());

    List<Datastream> cdcBstCandidates = sameSourceCandidates.stream()
        .filter(this::isCdcBootstrapRequiredStream)
        .collect(Collectors.toList());

    if (newStreamRequiresCdcBootstrap) {
      return findDedupCandidateForNewCdcBootstrapStream(newStream, cdcOnlyCandidates, cdcBstCandidates);
    } else {
      return findDedupCandidateForNewCdcOnlyStream(newStream, cdcOnlyCandidates, cdcBstCandidates);
    }
  }

  /**
   * Dedup logic for a new <em>CDC+BST</em> stream ({@code cdcBootstrapRequired=true}).
   *
   * <p>A CDC+BST stream starts consuming from an earlier source Kafka offset so it can replicate
   * historical data before catching up to the live tail.
   *
   * <ol>
   *   <li><b>Existing CDC+BST candidate</b> — dedup immediately, no timing check needed.
   *       Both streams have the same earlier-offset semantics so they can safely share a
   *       destination.</li>
   *   <li><b>Existing CDC-only candidate whose snapshot-refresh window has expired</b>
   *       ({@code creationTime + snapshotWorstRefreshDays < now}) — dedup against it.
   *       The CDC-only stream is old enough that its retained data covers the earlier offset
   *       position the new CDC+BST stream requires.</li>
   *   <li><b>No eligible candidate</b> — create a new independent destination. This happens
   *       when only a recent CDC-only stream exists: its offset history does not reach back
   *       far enough for the CDC+BST stream's earlier start position.</li>
   * </ol>
   *
   * @param newStream          the CDC+BST stream being created
   * @param cdcOnlyCandidates  existing CDC-only streams for the same source
   * @param cdcBstCandidates   existing CDC+BST streams for the same source
   * @return the chosen dedup target, or {@link Optional#empty()} if a new destination should be created
   */
  private Optional<Datastream> findDedupCandidateForNewCdcBootstrapStream(Datastream newStream,
      List<Datastream> cdcOnlyCandidates, List<Datastream> cdcBstCandidates) {

    // Step 1: prefer an existing CDC+BST stream — identical offset/type, always eligible.
    if (!cdcBstCandidates.isEmpty()) {
      Datastream candidate = cdcBstCandidates.get(0);
      LOG.info("Deduping new CDC+BST stream {} against existing CDC+BST stream {}",
          newStream.getName(), candidate.getName());
      return Optional.of(candidate);
    }

    // Step 2: fall back to a CDC-only stream whose snapshot-refresh window has expired.
    Optional<Datastream> eligibleCdcStream = cdcOnlyCandidates.stream()
        .filter(d -> hasWindowExpired(d, _snapshotWorstRefreshMs))
        .findFirst();
    if (eligibleCdcStream.isPresent()) {
      LOG.info("No CDC+BST stream found; deduping new CDC+BST stream {} against CDC stream {} "
              + "whose snapshot-refresh window has expired",
          newStream.getName(), eligibleCdcStream.get().getName());
      return eligibleCdcStream;
    }

    // Step 3: no eligible candidate — create a new destination.
    LOG.info("No eligible dedup candidate for new CDC+BST stream {}; a new destination will be created",
        newStream.getName());
    return Optional.empty();
  }

  /**
   * Dedup logic for a new <em>CDC-only</em> stream ({@code cdcBootstrapRequired=false} or absent).
   *
   * <p>A CDC-only stream starts from the <em>latest</em> Kafka offset and only reads events
   * produced after creation. It can share a destination with another stream only when their
   * effective offset positions are compatible.
   *
   * <ol>
   *   <li><b>Existing CDC+BST candidate whose grace window has expired</b>
   *       ({@code creationTime + newStreamGracePeriodMs < now}) — dedup against it.
   *       The CDC+BST stream started from an earlier offset to catch up on historical lag, but
   *       its grace window expiring means it has finished catching up and its consumer position
   *       has reached the live tail. At that point the two streams' effective offsets converge
   *       and sharing a destination is safe.</li>
   *   <li><b>Existing CDC-only candidate</b> — dedup immediately, no timing check needed.
   *       Both streams start from the latest offset so they are always compatible.</li>
   *   <li><b>No eligible candidate</b> — create a new independent destination. This happens
   *       when only a CDC+BST stream exists that is still within its grace window (still
   *       catching up); deduping now would cause the new CDC-only stream to miss events that
   *       the CDC+BST has not yet replicated.</li>
   * </ol>
   *
   * @param newStream          the CDC-only stream being created
   * @param cdcOnlyCandidates  existing CDC-only streams for the same source
   * @param cdcBstCandidates   existing CDC+BST streams for the same source
   * @return the chosen dedup target, or {@link Optional#empty()} if a new destination should be created
   */
  private Optional<Datastream> findDedupCandidateForNewCdcOnlyStream(Datastream newStream,
      List<Datastream> cdcOnlyCandidates, List<Datastream> cdcBstCandidates) {

    // Step 1: prefer a CDC+BST stream whose grace window has expired (bootstrap phase is done).
    Optional<Datastream> eligibleCdcBstStream = cdcBstCandidates.stream()
        .filter(d -> hasWindowExpired(d, _newStreamGracePeriodMs))
        .findFirst();
    if (eligibleCdcBstStream.isPresent()) {
      LOG.info("Deduping new CDC stream {} against CDC+BST stream {} whose grace window has expired",
          newStream.getName(), eligibleCdcBstStream.get().getName());
      return eligibleCdcBstStream;
    }

    // Step 2: fall back to an existing CDC-only stream.
    if (!cdcOnlyCandidates.isEmpty()) {
      Datastream candidate = cdcOnlyCandidates.get(0);
      LOG.info("Deduping new CDC stream {} against existing CDC stream {}",
          newStream.getName(), candidate.getName());
      return Optional.of(candidate);
    }

    // Step 3: no eligible candidate — create a new destination.
    LOG.info("No eligible dedup candidate for new CDC stream {}; a new destination will be created",
        newStream.getName());
    return Optional.empty();
  }

  /**
   * Returns {@code true} when the given stream carries the CDC-bootstrap metadata flag set to
   * {@code true}, indicating it is a CDC+BST stream.
   *
   * @param stream the stream to inspect
   * @return {@code true} if the stream is a CDC+BST stream, {@code false} otherwise
   */
  private boolean isCdcBootstrapRequiredStream(Datastream stream) {
    if (!stream.hasMetadata() || stream.getMetadata() == null) {
      return false;
    }
    return Boolean.parseBoolean(stream.getMetadata().getOrDefault(_cdcBootstrapRequiredMetadataKey, "false"));
  }

  /**
   * Returns {@code true} when the given stream's time window has expired, i.e.
   * {@code creationTime + windowMs < currentTime}.
   *
   * <p>When {@code windowMs} is {@code 0} (the default), the condition
   * {@code creationTime + 0 < currentTime} is always {@code true} for any existing stream,
   * so the window is treated as immediately expired and dedup is always permitted.
   *
   * <p>Returns {@code false} when {@code system.creation.ms} metadata is absent (cannot
   * determine age; conservatively treat as not expired).
   *
   * @param stream   the existing candidate stream
   * @param windowMs the window duration in milliseconds
   * @return {@code true} if the window has expired, {@code false} otherwise
   */
  private static boolean hasWindowExpired(Datastream stream, long windowMs) {
    if (!stream.hasMetadata() || stream.getMetadata() == null
        || !stream.getMetadata().containsKey(DatastreamMetadataConstants.CREATION_MS)) {
      return false;
    }
    long creationMs = Long.parseLong(stream.getMetadata().get(DatastreamMetadataConstants.CREATION_MS));
    return System.currentTimeMillis() > creationMs + windowMs;
  }
}
