package com.linkedin.datastream.connectors.kafka;

import java.util.function.BooleanSupplier;

import org.apache.kafka.common.TopicPartition;


/**
 * Contains metadata about a paused partition, including the resume criteria and reason for pause.
 */
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

  private final TopicPartition _topicPartition;
  private final BooleanSupplier _resumeCondition;
  private final Reason _reason;

  public PausedSourcePartitionMetadata(TopicPartition topicPartition, BooleanSupplier resumeCondition, Reason reason) {
    _topicPartition = topicPartition;
    _resumeCondition = resumeCondition;
    _reason = reason;
  }

  public TopicPartition getTopicPartition() {
    return _topicPartition;
  }

  public boolean shouldResume() {
    return _resumeCondition.getAsBoolean();
  }

  public Reason getReason() {
    return _reason;
  }

  @Override
  public String toString() {
    return String.format("TopicPartition %s is paused due to: %s", _topicPartition, _reason.getDescription());
  }
}
