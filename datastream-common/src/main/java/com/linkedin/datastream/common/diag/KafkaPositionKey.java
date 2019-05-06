/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.common.diag;

import java.time.Instant;
import java.util.Objects;

import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;
import org.codehaus.jackson.map.annotate.JsonDeserialize;
import org.codehaus.jackson.map.annotate.JsonSerialize;
import org.jetbrains.annotations.NotNull;

import com.linkedin.datastream.common.JsonUtils.InstantDeserializer;
import com.linkedin.datastream.common.JsonUtils.InstantSerializer;


/**
 * A KafkaPositionKey uniquely identifies an instantiation of a Kafka consumer within a Brooklin cluster.
 */
public class KafkaPositionKey implements PositionKey {

  /**
   * The position type.
   */
  private static final String KAFKA_POSITION_TYPE = "Kafka";

  /**
   * The Kafka topic we are consuming from.
   */
  private final String topic;

  /**
   * The Kafka partition we are consuming from.
   */
  private final int partition;

  /**
   * This Brooklin instance running the Kafka consumer.
   */
  private final String brooklinInstance;

  /**
   * A String that uniquely identifies the DatastreamTask which is running the Kafka consumer.
   */
  private final String brooklinTaskId;

  /**
   * The time at which consumption started.
   */
  @JsonSerialize(using = InstantSerializer.class)
  @JsonDeserialize(using = InstantDeserializer.class)
  private final Instant taskStartTime;

  /**
   * Constructs a KafkaPositionKey.
   *
   * @param topic the Kafka topic we are consuming from
   * @param partition the Kafka partition we are consuming from
   * @param brooklinInstance the Brooklin instance that is running the Kafka consumer
   * @param brooklinTaskId a unique identifier for the DatastreamTask which is running the consumer
   * @param taskStartTime the time at which consumption started
   */
  @JsonCreator
  public KafkaPositionKey(
      @JsonProperty("topic") @NotNull final String topic,
      @JsonProperty("partition") final int partition,
      @JsonProperty("brooklinInstance") @NotNull final String brooklinInstance,
      @JsonProperty("brooklinTaskId") @NotNull final String brooklinTaskId,
      @JsonProperty("taskStartTime") @NotNull final Instant taskStartTime) {
    this.topic = topic;
    this.partition = partition;
    this.brooklinInstance = brooklinInstance;
    this.brooklinTaskId = brooklinTaskId;
    this.taskStartTime = taskStartTime;
  }

  /**
   * Returns the position type.
   *
   * @return the position type
   */
  @NotNull
  public String getType() {
    return KAFKA_POSITION_TYPE;
  }

  /**
   * Returns the Kafka topic we are consuming from.
   *
   * @return the Kafka topic we are consuming from
   */
  @NotNull
  public String getTopic() {
    return topic;
  }

  /**
   * Returns the Kafka partition we are consuming from.
   *
   * @return the Kafka partition we are consuming from
   */
  public int getPartition() {
    return partition;
  }

  /**
   * Returns the Brooklin instance that is running the Kafka consumer.
   *
   * @return the Brooklin instance that is running the Kafka consumer
   */
  @NotNull
  public String getBrooklinInstance() {
    return brooklinInstance;
  }

  /**
   * Returns a String that uniquely identifies the DatastreamTask which is running the Kafka consumer.
   * @return a unique identifier for the DatastreamTask which is running the consumer
   */
  @NotNull
  public String getBrooklinTaskId() {
    return brooklinTaskId;
  }

  /**
   * Returns the time at which consumption started.
   * @return the time at which consumption started
   */
  @NotNull
  public Instant getTaskStartTime() {
    return taskStartTime;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    final KafkaPositionKey other = (KafkaPositionKey) obj;
    return partition == other.partition
        && Objects.equals(topic, other.topic)
        && Objects.equals(brooklinInstance, other.brooklinInstance)
        && Objects.equals(brooklinTaskId, other.brooklinTaskId)
        && Objects.equals(taskStartTime, other.taskStartTime);
  }

  @Override
  public int hashCode() {
    return Objects.hash(topic, partition, brooklinInstance, brooklinTaskId, taskStartTime);
  }
}