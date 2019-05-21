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
  private static final long serialVersionUID = 1L;

  /**
   * The Kafka topic we are consuming from.
   */
  private final String topic;

  /**
   * The Kafka partition we are consuming from.
   */
  private final int partition;

  /**
   * A String that matches the DatastreamTask's task prefix.
   */
  private final String datastreamTaskPrefix;

  /**
   * A String that uniquely identifies the DatastreamTask which is running the Kafka consumer.
   */
  private final String datastreamTaskName;

  /**
   * The time at which consumption started.
   */
  @JsonSerialize(using = InstantSerializer.class)
  @JsonDeserialize(using = InstantDeserializer.class)
  private final Instant connectorTaskStartTime;

  /**
   * Constructs a KafkaPositionKey.
   *
   * @param topic the Kafka topic we are consuming from
   * @param partition the Kafka partition we are consuming from
   * @param datastreamTaskPrefix the task prefix for the DatastreamTask which is running the consumer
   * @param datastreamTaskName a unique identifier for the DatastreamTask which is running the consumer
   * @param connectorTaskStartTime the time at which consumption started
   */
  @JsonCreator
  public KafkaPositionKey(
      @JsonProperty("topic") @NotNull final String topic,
      @JsonProperty("partition") final int partition,
      @JsonProperty("datastreamTaskPrefix") @NotNull final String datastreamTaskPrefix,
      @JsonProperty("datastreamTaskName") @NotNull final String datastreamTaskName,
      @JsonProperty("connectorTaskStartTime") @NotNull final Instant connectorTaskStartTime) {
    this.topic = topic;
    this.partition = partition;
    this.datastreamTaskPrefix = datastreamTaskPrefix;
    this.datastreamTaskName = datastreamTaskName;
    this.connectorTaskStartTime = connectorTaskStartTime;
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
   * Returns the task prefix of the DatastreamTask which is running the Kafka consumer.
   *
   * @return the task prefix of the DatastreamTask
   */
  @NotNull
  @Override
  public String getDatastreamTaskPrefix() {
    return datastreamTaskPrefix;
  }

  /**
   * Returns a unique id for the DatastreamTask assigned to the Kafka connector.
   *
   * @return an id that uniquely identifies a DatastreamTask
   */
  @NotNull
  @Override
  public String getDatastreamTaskName() {
    return datastreamTaskName;
  }

  /**
   * Returns the time at which consumption started.
   *
   * @return the time at which consumption started
   */
  @NotNull
  @Override
  public Instant getConnectorTaskStartTime() {
    return connectorTaskStartTime;
  }

  /**
   * {@inheritDoc}
   */
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
        && Objects.equals(datastreamTaskPrefix, other.datastreamTaskPrefix)
        && Objects.equals(datastreamTaskName, other.datastreamTaskName)
        && Objects.equals(connectorTaskStartTime, other.connectorTaskStartTime);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int hashCode() {
    return Objects.hash(topic, partition, datastreamTaskPrefix, datastreamTaskName, connectorTaskStartTime);
  }
}