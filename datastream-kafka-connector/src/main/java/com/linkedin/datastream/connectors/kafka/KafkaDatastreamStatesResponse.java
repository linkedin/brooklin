package com.linkedin.datastream.connectors.kafka;

import java.util.Map;
import java.util.Set;

import org.apache.kafka.common.TopicPartition;

import com.linkedin.datastream.common.JsonUtils;


public class KafkaDatastreamStatesResponse {

  private String _datastream;

  // Map of auto-paused Kafka topic partition to metadata about the paused partition
  private Map<TopicPartition, PausedSourcePartitionMetadata> _autoPausedPartitions;

  // Map of Kafka topic to set of manually paused partitions
  private Map<String, Set<String>> _manualPausedPartitions;

  public KafkaDatastreamStatesResponse(String datastream,
      Map<TopicPartition, PausedSourcePartitionMetadata> autoPausedPartitions,
      Map<String, Set<String>> manualPausedPartitions) {
    _datastream = datastream;
    _autoPausedPartitions = autoPausedPartitions;
    _manualPausedPartitions = manualPausedPartitions;
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

  public static String toJson(KafkaDatastreamStatesResponse obj) {
    return JsonUtils.toJson(obj);
  }
}
