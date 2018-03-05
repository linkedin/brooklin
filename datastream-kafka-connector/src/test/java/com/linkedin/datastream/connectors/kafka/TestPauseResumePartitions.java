package com.linkedin.datastream.connectors.kafka;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.kafka.common.TopicPartition;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.collect.Sets;


/**
 * Unit tests for helper functions inside AbstractKafkaBasedConnectorTask that determine which partitions should be
 * paused and resumed, based on the Datastream configuration and the auto-paused partitions sets.
 *
 * Tests in this class do not require any Kafka/ZK connections.
 */
public class TestPauseResumePartitions {

  @Test
  public void testPausePartitions() {
    String topic = "testPausePartitions";
    int numPartitions = 8;
    Set<String> partitions =
        IntStream.range(0, numPartitions).mapToObj(String::valueOf).collect(Collectors.toSet());
    Map<String, Set<String>> pausedSourcePartitionsConfig = new HashMap<>();
    Set<TopicPartition> autoPausedPartitions = Collections.emptySet();

    // 8 partitions are assigned
    Set<TopicPartition> assignedPartitions =
        IntStream.range(0, numPartitions).mapToObj(i -> new TopicPartition(topic, i)).collect(Collectors.toSet());

    // configure 8 partitions to pause and validate
    pausedSourcePartitionsConfig.put(topic, partitions);
    Set<TopicPartition> partitionsToPause =
        KafkaConnectorTask.determinePartitionsToPause(assignedPartitions, pausedSourcePartitionsConfig,
            autoPausedPartitions);
    Assert.assertEquals(partitionsToPause, assignedPartitions);

    // resume some partitions by removing them from config
    pausedSourcePartitionsConfig.get(topic).removeAll(Arrays.asList("2", "5"));
    Set<TopicPartition> expectedPartitionsToPause = new HashSet<>(partitionsToPause);
    expectedPartitionsToPause.removeAll(Arrays.asList(new TopicPartition(topic, 2), new TopicPartition(topic, 5)));
    partitionsToPause = KafkaConnectorTask.determinePartitionsToPause(assignedPartitions, pausedSourcePartitionsConfig,
        autoPausedPartitions);
    Assert.assertEquals(partitionsToPause, expectedPartitionsToPause);
  }

  @Test
  public void testAutoPausePartitions() {
    int numPartitions = 8;
    String topic = "testConfigAndAutoPausePartitions";

    // empty pause partitions config
    Map<String, Set<String>> pausedSourcePartitionsConfig = new HashMap<>();
    pausedSourcePartitionsConfig.put(topic, Collections.emptySet());

    // 8 partitions are assigned, 2 of them were auto-paused
    Set<TopicPartition> assignedPartitions =
        IntStream.range(0, numPartitions).mapToObj(i -> new TopicPartition(topic, i)).collect(Collectors.toSet());
    Set<TopicPartition> autoPausePartitions = new HashSet<>();
    autoPausePartitions.add(new TopicPartition(topic, 0));
    autoPausePartitions.add(new TopicPartition(topic, 4));
    // verify that 2 of the partitions were designated for pause
    Set<TopicPartition> partitionsToPause =
        KafkaConnectorTask.determinePartitionsToPause(assignedPartitions, pausedSourcePartitionsConfig,
            autoPausePartitions);
    Assert.assertEquals(partitionsToPause, autoPausePartitions);

    // auto-resume one of the partitions
    autoPausePartitions.remove(new TopicPartition(topic, 4));
    partitionsToPause = KafkaConnectorTask.determinePartitionsToPause(assignedPartitions, pausedSourcePartitionsConfig,
        autoPausePartitions);
    Assert.assertEquals(partitionsToPause, autoPausePartitions);
  }

  @Test
  public void testConfigAndAutoPausePartitions() {
    String topic = "testConfigAndAutoPausePartitions";

    // 8 partitions are assigned
    Set<TopicPartition> assignedPartitions =
        IntStream.range(0, 8).mapToObj(i -> new TopicPartition(topic, i)).collect(Collectors.toSet());

    // partitions 0-3 are configured for pause
    Set<String> configuredPartitions = IntStream.range(0, 4).mapToObj(String::valueOf).collect(Collectors.toSet());
    Map<String, Set<String>> pausedSourcePartitionsConfig = new HashMap<>();
    pausedSourcePartitionsConfig.put(topic, configuredPartitions);

    // partition 5 and 7 were auto-paused
    Set<TopicPartition> autoPausedPartitions = new HashSet<>();
    autoPausedPartitions.add(new TopicPartition(topic, 5));
    autoPausedPartitions.add(new TopicPartition(topic, 7));

    // verify that 6 of the partitions are designated for pause
    Set<TopicPartition> partitionsToPause =
        KafkaConnectorTask.determinePartitionsToPause(assignedPartitions, pausedSourcePartitionsConfig,
            autoPausedPartitions);
    Assert.assertEquals(partitionsToPause, IntStream.range(0, 8)
        .filter(i -> i != 4 && i != 6)
        .mapToObj(p -> new TopicPartition(topic, p))
        .collect(Collectors.toSet()));

    // test for resuming an auto-paused partition, by configuring it for pause and then unpausing it
    // resume the auto-paused partition 7
    pausedSourcePartitionsConfig.get(topic).add("7");
    // verify that partitionsToPause is the same
    partitionsToPause = KafkaConnectorTask.determinePartitionsToPause(assignedPartitions, pausedSourcePartitionsConfig,
        autoPausedPartitions);
    Assert.assertEquals(partitionsToPause, IntStream.range(0, 8)
        .filter(i -> i != 4 && i != 6)
        .mapToObj(p -> new TopicPartition(topic, p))
        .collect(Collectors.toSet()));
    // verify that partition 7 was removed from auto-pause list since it was added to configuration
    Assert.assertEquals(autoPausedPartitions, Sets.newHashSet(new TopicPartition(topic, 5)),
        "Partition should have been removed from auto-pause set, since it was added to configured set");

    // now resume partition 7, which was auto-paused then configured for pause
    pausedSourcePartitionsConfig.get(topic).remove("7");
    partitionsToPause = KafkaConnectorTask.determinePartitionsToPause(assignedPartitions, pausedSourcePartitionsConfig,
        autoPausedPartitions);
    Assert.assertEquals(partitionsToPause, IntStream.range(0, 8)
        .filter(i -> i != 4 && i != 6 && i != 7)
        .mapToObj(p -> new TopicPartition(topic, p))
        .collect(Collectors.toSet()));
  }

  @Test
  public void testUnassignedPartitionsRemovedFromPauseSets() {
    String topic = "testUnassignedPartitionsRemovedFromPauseSets";

    // 8 partitions are assigned
    Set<TopicPartition> assignedPartitions =
        IntStream.range(0, 8).mapToObj(i -> new TopicPartition(topic, i)).collect(Collectors.toSet());

    // no partitions configured for pause
    Map<String, Set<String>> pausedSourcePartitionsConfig = new HashMap<>();
    pausedSourcePartitionsConfig.put(topic, Collections.emptySet());

    // partition 5 (assigned) and partition 10 (not assigned) were auto-paused
    Set<TopicPartition> autoPausedPartitions = new HashSet<>();
    autoPausedPartitions.add(new TopicPartition(topic, 5));
    autoPausedPartitions.add(new TopicPartition(topic, 9));

    Set<TopicPartition> partitionsToPause = KafkaConnectorTask.determinePartitionsToPause(assignedPartitions,
        pausedSourcePartitionsConfig, autoPausedPartitions);
    // verify that partition 9 was removed from auto-pause list since it's no longer assigned
    Assert.assertEquals(partitionsToPause, Sets.newHashSet(new TopicPartition(topic, 5)),
        "Partition should have been removed from auto-pause set, since it is not in the assignment");
  }
}
