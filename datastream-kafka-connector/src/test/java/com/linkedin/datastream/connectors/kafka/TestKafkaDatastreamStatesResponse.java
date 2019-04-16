/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.connectors.kafka;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.common.TopicPartition;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.collect.Sets;

import com.linkedin.datastream.common.JsonUtils;
import com.linkedin.datastream.server.FlushlessEventProducerHandler;


/**
 * Tests for {@link KafkaDatastreamStatesResponse}
 */
@Test
public class TestKafkaDatastreamStatesResponse {

  @Test
  public void testJsonSerializeDeserialize() {

    String datastreamName = "testProcessDatastreamStates";
    String yummyTopic = "YummyPizza";
    String saltyTopic = "SaltyPizza";

    // build instance 1 results
    Set<TopicPartition> assignedPartitions = new HashSet<>();
    assignedPartitions.add(new TopicPartition(yummyTopic, 0));
    assignedPartitions.add(new TopicPartition(yummyTopic, 4));
    assignedPartitions.add(new TopicPartition(yummyTopic, 10));
    assignedPartitions.add(new TopicPartition(yummyTopic, 11));
    assignedPartitions.add(new TopicPartition(yummyTopic, 23));
    assignedPartitions.add(new TopicPartition(saltyTopic, 2));
    assignedPartitions.add(new TopicPartition(saltyTopic, 5));
    assignedPartitions.add(new TopicPartition(saltyTopic, 77));

    Map<TopicPartition, PausedSourcePartitionMetadata> autoPausedPartitions = new HashMap<>();
    Map<String, Set<String>> manualPausedPartitions = new HashMap<>();

    autoPausedPartitions.put(new TopicPartition(yummyTopic, 0),
        new PausedSourcePartitionMetadata(() -> false, PausedSourcePartitionMetadata.Reason.SEND_ERROR));
    autoPausedPartitions.put(new TopicPartition(yummyTopic, 10),
        new PausedSourcePartitionMetadata(() -> false, PausedSourcePartitionMetadata.Reason.SEND_ERROR));

    manualPausedPartitions.put(saltyTopic, Sets.newHashSet("2", "5", "77"));
    manualPausedPartitions.put(yummyTopic, Sets.newHashSet("4", "11", "23"));

    Map<FlushlessEventProducerHandler.SourcePartition, Long> inflightMsgCounts = new HashMap<>();
    inflightMsgCounts.put(new FlushlessEventProducerHandler.SourcePartition(yummyTopic, 4), 6L);

    KafkaDatastreamStatesResponse processResponse =
        new KafkaDatastreamStatesResponse(datastreamName, autoPausedPartitions, manualPausedPartitions,
            assignedPartitions, inflightMsgCounts);

    String serialized = JsonUtils.toJson(processResponse);

    KafkaDatastreamStatesResponse deserialized = KafkaDatastreamStatesResponse.fromJson(serialized);
    Assert.assertEquals(deserialized.getAssignedTopicPartitions(), assignedPartitions, "Assigned partitions mismatch");
    Assert.assertEquals(deserialized.getInFlightMessageCounts(), inflightMsgCounts,
        "In-flight message counts mismatch");
    Assert.assertEquals(deserialized.getManualPausedPartitions(), manualPausedPartitions,
        "Manually paused partitions mismatch");
    Assert.assertEquals(deserialized.getAutoPausedPartitions(), autoPausedPartitions,
        "Auto-paused partitions mismatch");
    Assert.assertEquals(deserialized.getDatastream(), datastreamName, "Datastream name mismatch ");
  }
}
