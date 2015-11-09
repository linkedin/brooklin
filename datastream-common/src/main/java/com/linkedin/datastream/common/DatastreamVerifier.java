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
    Objects.requireNonNull(datastream.getTarget(), "invalid datastream target");
    Objects.requireNonNull(datastream.getTarget().getKafkaConnection(), "invalid kafka connection");
    KafkaConnection conn = datastream.getTarget().getKafkaConnection();
    Objects.requireNonNull(conn.getMetadataBrokers(), "invalid Kafka metadata brokers");
    Objects.requireNonNull(conn.getTopicName(), "invalid Kafka topic name");
    if (conn.getPartitions() <= 0) {
      throw new IllegalArgumentException("invalid Kafka topic partition count.");
    }
  }
}
