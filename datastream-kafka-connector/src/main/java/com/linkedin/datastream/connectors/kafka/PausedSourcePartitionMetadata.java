/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.connectors.kafka;

import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.function.BooleanSupplier;

import org.codehaus.jackson.annotate.JsonPropertyOrder;


/**
 * Contains metadata about a paused partition, including the resume criteria and reason for pause.
 */
@JsonPropertyOrder({"reason", "description"})
public class PausedSourcePartitionMetadata {

  /**
   * Enum representing reason for which a partition is paused.
   */
  public enum Reason {
    EXCEEDED_MAX_IN_FLIGHT_MSG_THRESHOLD("Number of in-flight messages for partition exceeded threshold"),
    SEND_ERROR("Failed to produce messages from this partition"),
    TOPIC_NOT_CREATED("Topic not created on the destination side");

    private final String _description;

    Reason(String description) {
      _description = description;
    }

    String getDescription() {
      return _description;
    }
  }

  private BooleanSupplier _resumeCondition = null;
  private Reason _reason = null;
  private String _description = null;

  /**
   * Empty constructor.
   */
  public PausedSourcePartitionMetadata() {

  }

  /**
   * Constructor for PausedSourcePartitionMetadata.
   * @param resumeCondition BooleanSupplier that is used to evaluate if the partition that PausedSourcePartitionMetadata
   *                        represents should now resume.
   * @param reason Reason for which partition that PausedSourcePartitionMetadata represents is being paused.
   */
  public PausedSourcePartitionMetadata(BooleanSupplier resumeCondition, Reason reason) {
    _resumeCondition = resumeCondition;
    _reason = reason;
    _description = reason.getDescription();
  }

  /**
   * Evaluates if the partition that PausedSourcePartitionMetadata represents should now resume.
   * @return True if the partition should resume, false otherwise.
   */
  public boolean shouldResume() {
    return _resumeCondition.getAsBoolean();
  }

  public Reason getReason() {
    return _reason;
  }

  public void setReason(Reason reason) {
    _reason = reason;
  }

  public String getDescription() {
    return _description;
  }

  public void setDescription(String description) {
    _description = description;
  }

  public void setResumeCondition(BooleanSupplier resumeCondition) {
    _resumeCondition = resumeCondition;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    PausedSourcePartitionMetadata that = (PausedSourcePartitionMetadata) o;
    return _reason == that._reason && Objects.equals(_description, that._description);
  }

  @Override
  public int hashCode() {
    return Objects.hash(_reason, _description);
  }

  @Override
  public String toString() {
    return _description == null ? "" : _description.toString();
  }

  /**
   * Creates a PausedSourcePartitionMetadata with "SEND_ERROR" reason. It represents a pause partition because of errors
   * seen while sending data to that partition.
   * @param start Start time when the partition was paused.
   * @param pauseDuration Duration for which the partition should be paused. The duration is used to check if partition
   *                      should be unpaused.
   * @return Instance of PausedSourcePartitionMetadata.
   */
  public static PausedSourcePartitionMetadata sendError(Instant start, Duration pauseDuration) {
    return new PausedSourcePartitionMetadata(() -> Duration.between(start, Instant.now()).compareTo(pauseDuration) > 0,
        Reason.SEND_ERROR);
  }
}
