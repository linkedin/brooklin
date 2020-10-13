/**
 *  Copyright 2020 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.connectors.kafka;

import java.util.HashMap;
import java.util.Map;

import org.codehaus.jackson.annotate.JsonProperty;


/**
 * Response structure used for consumer offsets diagnostics-endpoint requests
 */
public class KafkaConsumerOffsetsResponse {
  private final String _consumerGroupId;
  private final Map<String, Map<Integer, Long>> _consumedOffsets;
  private final Map<String, Map<Integer, Long>> _committedOffsets;

  /**
   * Constructor for {@link KafkaConsumerOffsetsResponse}
   * @param consumedOffsets Consumed offsets for all topic partitions
   * @param committedOffsets Committed offsets for all topic partitions
   * @param consumerGroupId Consumer group ID
   */
  public KafkaConsumerOffsetsResponse(@JsonProperty("consumedOffsets") Map<String, Map<Integer, Long>> consumedOffsets,
      @JsonProperty("committedOffsets") Map<String, Map<Integer, Long>> committedOffsets,
      @JsonProperty("consumerGroupId") String consumerGroupId) {
    _consumerGroupId = consumerGroupId;
    _consumedOffsets = consumedOffsets;
    _committedOffsets = committedOffsets;
  }

  /**
   * Constructor for {@link KafkaConsumerOffsetsResponse}
   * @param consumerGroupId Consumer group ID
   */
  public KafkaConsumerOffsetsResponse(String consumerGroupId) {
    this(new HashMap<>(), new HashMap<>(), consumerGroupId);
  }

  public Map<String, Map<Integer, Long>> getConsumedOffsets() {
    return _consumedOffsets;
  }

  public Map<String, Map<Integer, Long>> getCommittedOffsets() {
    return _committedOffsets;
  }

  public String getConsumerGroupId() {
    return _consumerGroupId;
  }
}
