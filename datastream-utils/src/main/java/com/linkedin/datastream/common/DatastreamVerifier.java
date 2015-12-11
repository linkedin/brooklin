package com.linkedin.datastream.common;

import org.apache.commons.lang.Validate;


/**
 * Simple utility class for checking the validity of Datastream at various stages.
 */
public final class DatastreamVerifier {
  private DatastreamVerifier() {
  }

  public static void checkNew(Datastream datastream) {
    Validate.notNull(datastream, "invalid datastream");
    Validate.notNull(datastream.getSource(), "invalid datastream source");
    Validate.notNull(datastream.getName(), "invalid datastream name");
    Validate.notNull(datastream.getConnectorType(), "invalid datastream connector type");
  }

  public static void checkExisting(Datastream datastream) {
    checkNew(datastream);
    Validate.notNull(datastream.getDestination(), "invalid datastream destination");
    Validate.notNull(datastream.getDestination().getConnectionString(), "invalid destination connection");
    Validate.notNull(datastream.getDestination().getPartitions(), "invalid destination partitions");
    if (datastream.getDestination().getPartitions() <= 0) {
      throw new IllegalArgumentException("invalid destination partition count.");
    }
  }
}
