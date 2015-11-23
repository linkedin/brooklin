package com.linkedin.datastream.common;

import java.util.Objects;


/**
 * Simple utility class for checking the validity of Datastream at various stages.
 */
public final class DatastreamVerifier {
  private DatastreamVerifier() {
  }

  public static void checkNew(Datastream datastream) {
    Objects.requireNonNull(datastream, "invalid datastream");
    Objects.requireNonNull(datastream.getSource(), "invalid datastream source");
    Objects.requireNonNull(datastream.getName(), "invalid datastream name");
    Objects.requireNonNull(datastream.getConnectorType(), "invalid datastream connector type");
  }

  public static void checkExisting(Datastream datastream) {
    checkNew(datastream);
    Objects.requireNonNull(datastream.getDestination(), "invalid datastream destination");
    Objects.requireNonNull(datastream.getDestination().getConnectionString(), "invalid destination connection");
    Objects.requireNonNull(datastream.getDestination().getPartitions(), "invalid destination partitions");
    if (datastream.getDestination().getPartitions() <= 0) {
      throw new IllegalArgumentException("invalid destination partition count.");
    }
  }
}
