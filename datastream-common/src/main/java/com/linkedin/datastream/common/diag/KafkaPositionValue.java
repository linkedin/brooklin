/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.common.diag;

import java.time.Instant;
import java.util.Objects;

import org.codehaus.jackson.map.annotate.JsonDeserialize;
import org.codehaus.jackson.map.annotate.JsonSerialize;
import org.jetbrains.annotations.Nullable;

import com.linkedin.datastream.common.JsonUtils.InstantDeserializer;
import com.linkedin.datastream.common.JsonUtils.InstantSerializer;


/**
 * A KafkaPositionValue describes information about a Kafka consumer's status and current position when reading from a
 * specific TopicPartition.
 */
public class KafkaPositionValue implements PositionValue {
  private static final long serialVersionUID = 1L;

  /**
   * The latest offset (the offset of the last produced message) on the Kafka broker for this TopicPartition. If the
   * consumer is also at this position, then it is completely caught up and has no more messages to process.
   */
  private Long brokerOffset;

  /**
   * The current offset that the Kafka consumer has for this TopicPartition. When the consumer calls poll(), if it
   * receives records, the first record that it receives will have this offset.
   */
  private Long consumerOffset;

  /**
   * The time at which the Kafka consumer was assigned this TopicPartition.
   */
  @JsonSerialize(using = InstantSerializer.class)
  @JsonDeserialize(using = InstantDeserializer.class)
  private Instant assignmentTime;

  /**
   * The timestamp of the last record that we got from poll().
   */
  @JsonSerialize(using = InstantSerializer.class)
  @JsonDeserialize(using = InstantDeserializer.class)
  private Instant lastRecordReceivedTimestamp;

  /**
   * The last time that we queried a broker for its latest offset -- either by reading metrics data received from
   * poll(), or by querying the broker's latest offset via endOffsets().
   */
  @JsonSerialize(using = InstantSerializer.class)
  @JsonDeserialize(using = InstantDeserializer.class)
  private Instant lastBrokerQueriedTime;

  /**
   * The last time we called poll() and received messages.
   */
  @JsonSerialize(using = InstantSerializer.class)
  @JsonDeserialize(using = InstantDeserializer.class)
  private Instant lastNonEmptyPollTime;

  /**
   * Default constructor.
   */
  public KafkaPositionValue() {
  }

  /**
   * Returns the latest offset (the offset of the last produced message) on the Kafka broker for this TopicPartition. If
   * the consumer is also at this position, then it is completely caught up and has no more messages to process.
   *
   * @return the latest broker offset
   */
  @Nullable
  public Long getBrokerOffset() {
    return brokerOffset;
  }

  /**
   * Sets the latest offset (the offset of the last produced message) on the Kafka broker for this TopicPartition.
   *
   * @param brokerOffset the latest broker offset
   */
  public void setBrokerOffset(Long brokerOffset) {
    this.brokerOffset = brokerOffset;
  }

  /**
   * Returns the current offset that the Kafka consumer has for this TopicPartition. When the consumer calls poll(), if
   * it receives records, the first record that it receives will have this offset.
   *
   * @return the consumer's position
   */
  @Nullable
  public Long getConsumerOffset() {
    return consumerOffset;
  }

  /**
   * Sets the current offset that the Kafka consumer has for this TopicPartition, as would be returned by position().
   *
   * @param consumerOffset the consumer's position
   */
  public void setConsumerOffset(Long consumerOffset) {
    this.consumerOffset = consumerOffset;
  }

  /**
   * Returns the time at which the Kafka consumer was assigned this TopicPartition.
   *
   * @return the time at the consumer was assigned this partition
   */
  @Nullable
  public Instant getAssignmentTime() {
    return assignmentTime;
  }

  /**
   * Sets the time at which the Kafka consumer was assigned this TopicPartition.
   *
   * @param assignmentTime the time at the consumer was assigned this partition
   */
  public void setAssignmentTime(Instant assignmentTime) {
    this.assignmentTime = assignmentTime;
  }

  /**
   * Returns the timestamp of the last record that we got from poll().
   *
   * @return the timestamp of the last record received
   */
  @Nullable
  public Instant getLastRecordReceivedTimestamp() {
    return lastRecordReceivedTimestamp;
  }

  /**
   * Sets the timestamp of the last record that we got from poll().
   *
   * @param lastRecordReceivedTimestamp the timestamp of the last record received
   */
  public void setLastRecordReceivedTimestamp(Instant lastRecordReceivedTimestamp) {
    this.lastRecordReceivedTimestamp = lastRecordReceivedTimestamp;
  }

  /**
   * Returns the last time that we queried a broker for its latest offset.
   *
   * @return the last time that we queried a broker for its latest offset
   */
  @Nullable
  public Instant getLastBrokerQueriedTime() {
    return lastBrokerQueriedTime;
  }

  /**
   * Sets the last time that we queried a broker for its latest offset.
   * @param lastBrokerQueriedTime the last time that we queried a broker for its latest offset
   */
  public void setLastBrokerQueriedTime(Instant lastBrokerQueriedTime) {
    this.lastBrokerQueriedTime = lastBrokerQueriedTime;
  }

  /**
   * Returns the last time we called poll() and received messages.
   *
   * @return the last time we called poll() and received messages
   */
  @Nullable
  public Instant getLastNonEmptyPollTime() {
    return lastNonEmptyPollTime;
  }

  /**
   * Sets the last time we called poll() and received messages.
   *
   * @param lastNonEmptyPollTime the last time we called poll() and received messages
   */
  public void setLastNonEmptyPollTime(Instant lastNonEmptyPollTime) {
    this.lastNonEmptyPollTime = lastNonEmptyPollTime;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    final KafkaPositionValue other = (KafkaPositionValue) obj;
    return Objects.equals(brokerOffset, other.brokerOffset)
        && Objects.equals(consumerOffset, other.consumerOffset)
        && Objects.equals(assignmentTime, other.assignmentTime)
        && Objects.equals(lastRecordReceivedTimestamp, other.lastRecordReceivedTimestamp)
        && Objects.equals(lastBrokerQueriedTime, other.lastBrokerQueriedTime)
        && Objects.equals(lastNonEmptyPollTime, other.lastNonEmptyPollTime);
  }

  @Override
  public int hashCode() {
    return Objects.hash(brokerOffset, consumerOffset, assignmentTime, lastRecordReceivedTimestamp, lastBrokerQueriedTime,
        lastNonEmptyPollTime);
  }
}