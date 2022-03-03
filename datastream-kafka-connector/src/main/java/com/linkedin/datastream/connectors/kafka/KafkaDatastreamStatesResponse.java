/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.connectors.kafka;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.common.TopicPartition;

import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.Version;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.KeyDeserializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.ser.std.ToStringSerializer;

import com.linkedin.datastream.common.JsonUtils;
import com.linkedin.datastream.server.FlushlessEventProducerHandler;


/**
 * Holds information about the states of the various datastreams in a Brooklin instance.
 * Used to respond to requests made to the ServerComponentHealthResources diagnostic endpoint.
 * @see AbstractKafkaConnector#process(String)
 */
@JsonPropertyOrder({"datastream", "assignedTopicPartitions", "autoPausedPartitions", "manualPausedPartitions", "inFlightMessageCounts"})
public class KafkaDatastreamStatesResponse {

  private String _datastream;

  // Map of auto-paused Kafka topic partition to metadata about the paused partition
  private Map<TopicPartition, PausedSourcePartitionMetadata> _autoPausedPartitions;

  // Map of Kafka topic to set of manually paused partitions
  private Map<String, Set<String>> _manualPausedPartitions;

  // Set of assigned TopicPartitions
  private Set<TopicPartition> _assignedTopicPartitions;

  // Map of source partition to number of in-flight message counts
  private Map<FlushlessEventProducerHandler.SourcePartition, Long> _inFlightMessageCounts;

  /**
   * Constructor
   */
  public KafkaDatastreamStatesResponse() {

  }

  /**
   * Constructor
   * @param datastream datastream name
   * @param autoPausedPartitions a map of paused Kafka topic partitions and their metadata (e.g. reason for pause)
   * @param manualPausedPartitions a map of topic to partitions that are configured for pause
   * @param assignedTopicPartitions all assigned Kafka topic partitions
   */
  public KafkaDatastreamStatesResponse(String datastream,
      Map<TopicPartition, PausedSourcePartitionMetadata> autoPausedPartitions,
      Map<String, Set<String>> manualPausedPartitions, Set<TopicPartition> assignedTopicPartitions) {
    this(datastream, autoPausedPartitions, manualPausedPartitions, assignedTopicPartitions, Collections.emptyMap());
  }

  /**
   * Constructor
   * @param datastream datastream name
   * @param autoPausedPartitions a map of paused Kafka topic partitions and their metadata (e.g. reason for pause)
   * @param manualPausedPartitions a map of topic to partitions that are configured for pause
   * @param assignedTopicPartitions all assigned Kafka topic partitions
   * @param inFlightMessageCounts a map of all source partitions to their inflight message counts
   * @see FlushlessEventProducerHandler#getInFlightMessagesCounts()
   */
  public KafkaDatastreamStatesResponse(String datastream,
      Map<TopicPartition, PausedSourcePartitionMetadata> autoPausedPartitions,
      Map<String, Set<String>> manualPausedPartitions, Set<TopicPartition> assignedTopicPartitions,
      Map<FlushlessEventProducerHandler.SourcePartition, Long> inFlightMessageCounts) {
    _datastream = datastream;
    _autoPausedPartitions = autoPausedPartitions;
    _manualPausedPartitions = manualPausedPartitions;
    _assignedTopicPartitions = assignedTopicPartitions;
    _inFlightMessageCounts = inFlightMessageCounts;
  }

  /**
   * Serialize to JSON
   */
  public static String toJson(KafkaDatastreamStatesResponse obj) {
    return JsonUtils.toJson(obj);
  }

  /**
   * Deserialize from JSON
   */
  public static KafkaDatastreamStatesResponse fromJson(String json) {
    SimpleModule simpleModule = new SimpleModule("KafkaDatastreamStatesResponseModule", Version.unknownVersion());
    simpleModule.addKeyDeserializer(FlushlessEventProducerHandler.SourcePartition.class, SourcePartitionDeserializer.getInstance());
    simpleModule.addKeyDeserializer(TopicPartition.class, TopicPartitionKeyDeserializer.getInstance());
    simpleModule.addDeserializer(TopicPartition.class, TopicPartitionDeserializer.getInstance());
    ObjectMapper mapper = new ObjectMapper();
    mapper.registerModule(simpleModule);
    mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    return JsonUtils.fromJson(json, KafkaDatastreamStatesResponse.class, mapper);
  }

  private static TopicPartition topicPartitionFromString(String tp) {
    int partitionDelimiterIndex = tp.lastIndexOf("-");
    String source = tp.substring(0, partitionDelimiterIndex);
    String partition = tp.substring(partitionDelimiterIndex + 1);
    return new TopicPartition(source, Integer.parseInt(partition));
  }

  public String getDatastream() {
    return _datastream;
  }

  public void setDatastream(String datastream) {
    _datastream = datastream;
  }

  public Map<TopicPartition, PausedSourcePartitionMetadata> getAutoPausedPartitions() {
    return _autoPausedPartitions;
  }

  public void setAutoPausedPartitions(Map<TopicPartition, PausedSourcePartitionMetadata> autoPausedPartitions) {
    _autoPausedPartitions = autoPausedPartitions;
  }

  public Map<String, Set<String>> getManualPausedPartitions() {
    return _manualPausedPartitions;
  }

  public void setManualPausedPartitions(Map<String, Set<String>> manualPausedPartitions) {
    _manualPausedPartitions = manualPausedPartitions;
  }

  public Map<FlushlessEventProducerHandler.SourcePartition, Long> getInFlightMessageCounts() {
    return _inFlightMessageCounts;
  }

  public void setInFlightMessageCounts(Map<FlushlessEventProducerHandler.SourcePartition, Long> inFlightMessageCounts) {
    _inFlightMessageCounts = inFlightMessageCounts;
  }

  @JsonSerialize(contentUsing = ToStringSerializer.class)
  public Set<TopicPartition> getAssignedTopicPartitions() {
    return _assignedTopicPartitions;
  }

  public void setAssignedTopicPartitions(Set<TopicPartition> assignedTopicPartitions) {
    _assignedTopicPartitions = assignedTopicPartitions;
  }

  private static class SourcePartitionDeserializer extends KeyDeserializer {
    private static final SourcePartitionDeserializer INSTANCE = new SourcePartitionDeserializer();

    static SourcePartitionDeserializer getInstance() {
      return INSTANCE;
    }

    @Override
    public Object deserializeKey(String key, DeserializationContext ctxt) throws IOException {
      TopicPartition tp = topicPartitionFromString(key);
      return new FlushlessEventProducerHandler.SourcePartition(tp.topic(), tp.partition());
    }
  }

  private static class TopicPartitionKeyDeserializer extends KeyDeserializer {
    private static final TopicPartitionKeyDeserializer INSTANCE = new TopicPartitionKeyDeserializer();

    static TopicPartitionKeyDeserializer getInstance() {
      return INSTANCE;
    }

    @Override
    public Object deserializeKey(String key, DeserializationContext ctxt) throws IOException {
      return topicPartitionFromString(key);
    }
  }

  private static class TopicPartitionDeserializer extends JsonDeserializer<TopicPartition> {
    private static final TopicPartitionDeserializer INSTANCE = new TopicPartitionDeserializer();

    static TopicPartitionDeserializer getInstance() {
      return INSTANCE;
    }

    @Override
    public TopicPartition deserialize(JsonParser jp, DeserializationContext ctxt)
        throws IOException {
      String topicPartition = ((JsonNode) jp.getCodec().readTree(jp)).asText();
      return topicPartitionFromString(topicPartition);
    }
  }
}
