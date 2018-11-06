package com.linkedin.datastream.connectors.kafka.mirrormaker;

import java.util.Optional;

import com.linkedin.datastream.common.Datastream;


/**
 * Holds constants and helpers for accessing Kafka MirrorMaker datastream metadata
 */
public final class KafkaMirrorMakerDatastreamMetadata {

  public static final String IDENTITY_PARTITIONING_ENABLED = "system.destination.identityPartitioningEnabled";

  /**
   * Getter for whether identity partition is enabled for this datastream. The default is false.
   */
  public static boolean isIdentityPartitioningEnabled(Datastream datastream) {
    return Boolean.parseBoolean(Optional.ofNullable(datastream)
        .map(d -> d.getMetadata().get(IDENTITY_PARTITIONING_ENABLED))
        .orElse(Boolean.FALSE.toString()));
  }

}
