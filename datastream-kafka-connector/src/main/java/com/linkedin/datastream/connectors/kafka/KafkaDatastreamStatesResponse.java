package com.linkedin.datastream.connectors.kafka;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.common.TopicPartition;
import org.codehaus.jackson.annotate.JsonPropertyOrder;
import org.codehaus.jackson.map.annotate.JsonSerialize;
import org.codehaus.jackson.map.ser.ToStringSerializer;

import com.linkedin.datastream.common.JsonUtils;
import com.linkedin.datastream.server.FlushlessEventProducerHandler;


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

  public KafkaDatastreamStatesResponse(String datastream,
      Map<TopicPartition, PausedSourcePartitionMetadata> autoPausedPartitions,
      Map<String, Set<String>> manualPausedPartitions, Set<TopicPartition> assignedTopicPartitions) {
    this(datastream, autoPausedPartitions, manualPausedPartitions, assignedTopicPartitions, Collections.emptyMap());
  }

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

  public String getDatastream() {
    return _datastream;
  }

  public Map<TopicPartition, PausedSourcePartitionMetadata> getAutoPausedPartitions() {
    return _autoPausedPartitions;
  }

  public Map<String, Set<String>> getManualPausedPartitions() {
    return _manualPausedPartitions;
  }

  public Map<FlushlessEventProducerHandler.SourcePartition, Long> getInFlightMessageCounts() {
    return _inFlightMessageCounts;
  }

  @JsonSerialize(contentUsing = ToStringSerializer.class)
  public Set<TopicPartition> getAssignedTopicPartitions() {
    return _assignedTopicPartitions;
  }

  public static String toJson(KafkaDatastreamStatesResponse obj) {
    return JsonUtils.toJson(obj);
  }
}
