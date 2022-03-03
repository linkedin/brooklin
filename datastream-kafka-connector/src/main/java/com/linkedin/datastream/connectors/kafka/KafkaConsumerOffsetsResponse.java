/**
 *  Copyright 2020 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.connectors.kafka;

import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonProperty;


/**
 * Response structure used for consumer offsets diagnostics-endpoint requests
 */
public class KafkaConsumerOffsetsResponse {
  private final String _consumerGroupId;
  private final String _datastreamName;
  private final Map<String, Map<Integer, Long>> _consumedOffsets;
  private final Map<String, Map<Integer, Long>> _committedOffsets;
  private final Map<String, Map<Integer, Long>> _consumptionLagMap;

  /**
   * Constructor for {@link KafkaConsumerOffsetsResponse}
   * @param consumedOffsets Consumed offsets for all topic partitions
   * @param committedOffsets Committed offsets for all topic partitions
   * @param consumerGroupId Consumer group ID
   * @param datastreamName Datastream name
   */
  public KafkaConsumerOffsetsResponse(
      @JsonProperty("consumedOffsets") Map<String, Map<Integer, Long>> consumedOffsets,
      @JsonProperty("committedOffsets") Map<String, Map<Integer, Long>> committedOffsets,
      @JsonProperty("consumptionLagMap") Map<String, Map<Integer, Long>> consumptionLagMap,
      @JsonProperty("consumerGroupId") String consumerGroupId,
      @JsonProperty("datastreamName") String datastreamName) {
    _consumerGroupId = consumerGroupId;
    _consumedOffsets = consumedOffsets;
    _committedOffsets = committedOffsets;
    _consumptionLagMap = consumptionLagMap;
    _datastreamName = datastreamName;
  }

  /**
   * Constructor for {@link KafkaConsumerOffsetsResponse}
   * @param consumerGroupId Consumer group ID
   */
  public KafkaConsumerOffsetsResponse(String consumerGroupId, String datastreamName) {
    this(new HashMap<>(), new HashMap<>(), new HashMap<>(), consumerGroupId, datastreamName);
  }

  /**
   * Gets the consumed offsets
   */
  public Map<String, Map<Integer, Long>> getConsumedOffsets() {
    return _consumedOffsets;
  }

  /**
   * Gets the committed offsets
   */
  public Map<String, Map<Integer, Long>> getCommittedOffsets() {
    return _committedOffsets;
  }

  /**
   * Gets the consumption lag map
   */
  public Map<String, Map<Integer, Long>> getConsumptionLagMap() {
    return _consumptionLagMap;
  }

  /**
   * Gets consumer group identifier
   */
  public String getConsumerGroupId() {
    return _consumerGroupId;
  }

  /**
   * Gets the datastream name
   */
  public String getDatastreamName() {
    return _datastreamName;
  }
}
