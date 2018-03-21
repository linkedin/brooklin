package com.linkedin.datastream.connectors.kafka;

import java.time.Duration;
import java.time.Instant;
import java.util.function.BooleanSupplier;

import org.codehaus.jackson.annotate.JsonPropertyOrder;


/**
 * Contains metadata about a paused partition, including the resume criteria and reason for pause.
 */
@JsonPropertyOrder({"reason", "description"})
public class PausedSourcePartitionMetadata {

  public enum Reason {
    EXCEEDED_MAX_IN_FLIGHT_MSG_THRESHOLD("Number of in-flight messages for partition exceeded threshold"),
    SEND_ERROR("Failed to produce messages from this partition");

    private final String _description;

    Reason(String description) {
      _description = description;
    }

    String getDescription() {
      return _description;
    }
  }

  private final BooleanSupplier _resumeCondition;
  private final Reason _reason;
  private final String _description;

  public PausedSourcePartitionMetadata(BooleanSupplier resumeCondition, Reason reason) {
    _resumeCondition = resumeCondition;
    _reason = reason;
    _description = reason.getDescription();
  }

  public boolean shouldResume() {
    return _resumeCondition.getAsBoolean();
  }

  public Reason getReason() {
    return _reason;
  }

  public String getDescription() {
    return _description;
  }

  public static PausedSourcePartitionMetadata sendError(Instant start, Duration pauseDuration) {
    return new PausedSourcePartitionMetadata(() -> Duration.between(start, Instant.now()).compareTo(pauseDuration) > 0,
        Reason.SEND_ERROR);
  }

}
