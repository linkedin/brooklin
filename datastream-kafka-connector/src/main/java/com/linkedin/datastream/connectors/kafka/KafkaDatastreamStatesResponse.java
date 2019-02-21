package com.linkedin.datastream.connectors.kafka;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.common.TopicPartition;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.JsonProcessingException;
import org.codehaus.jackson.Version;
import org.codehaus.jackson.annotate.JsonPropertyOrder;
import org.codehaus.jackson.map.DeserializationConfig;
import org.codehaus.jackson.map.DeserializationContext;
import org.codehaus.jackson.map.JsonDeserializer;
import org.codehaus.jackson.map.KeyDeserializer;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.annotate.JsonSerialize;
import org.codehaus.jackson.map.module.SimpleModule;
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

  public KafkaDatastreamStatesResponse() {

  }

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

  public static String toJson(KafkaDatastreamStatesResponse obj) {
    return JsonUtils.toJson(obj);
  }

  public static KafkaDatastreamStatesResponse fromJson(String json) {
    SimpleModule simpleModule = new SimpleModule("KafkaDatastreamStatesResponseModule", Version.unknownVersion());
    simpleModule.addKeyDeserializer(FlushlessEventProducerHandler.SourcePartition.class, SourcePartitionDeserializer.getInstance());
    simpleModule.addKeyDeserializer(TopicPartition.class, TopicPartitionKeyDeserializer.getInstance());
    simpleModule.addDeserializer(TopicPartition.class, TopicPartitionDeserializer.getInstance());
    ObjectMapper mapper = new ObjectMapper();
    mapper.registerModule(simpleModule);
    mapper.configure(DeserializationConfig.Feature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    return JsonUtils.fromJson(json, KafkaDatastreamStatesResponse.class, mapper);
  }

  private static TopicPartition topicPartitionFromString(String tp) {
    int partitionDelimiterIndex = tp.lastIndexOf("-");
    String source = tp.substring(0, partitionDelimiterIndex);
    String partition = tp.substring(partitionDelimiterIndex + 1, tp.length());
    return new TopicPartition(source, Integer.valueOf(partition));
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
    public Object deserializeKey(String key, DeserializationContext ctxt) throws IOException, JsonProcessingException {
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
    public Object deserializeKey(String key, DeserializationContext ctxt) throws IOException, JsonProcessingException {
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
        throws IOException, JsonProcessingException {
      String topicPartition = jp.getCodec().readTree(jp).getTextValue();
      return topicPartitionFromString(topicPartition);
    }
  }
}
