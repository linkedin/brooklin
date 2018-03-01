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
        KafkaConnectorTask.getPartitionsToPause(assignedPartitions, Collections.emptySet(),
            pausedSourcePartitionsConfig, autoPausedPartitions);
    Assert.assertEquals(partitionsToPause, assignedPartitions);

    // resume some partitions by removing them from config
    pausedSourcePartitionsConfig.get(topic).removeAll(Arrays.asList("2", "5"));
    Set<TopicPartition> currentPausedPartitions = new HashSet<>(partitionsToPause);
    partitionsToPause = KafkaConnectorTask.getPartitionsToPause(assignedPartitions, currentPausedPartitions,
        pausedSourcePartitionsConfig, autoPausedPartitions);
    Assert.assertTrue(partitionsToPause.isEmpty(), "There should be no partitions to pause");
    Set<TopicPartition> partitionsToResume =
        KafkaConnectorTask.getPartitionsToResume(currentPausedPartitions, pausedSourcePartitionsConfig,
            autoPausedPartitions);
    Assert.assertEquals(partitionsToResume.size(), 2);
    Assert.assertTrue(partitionsToResume.contains(new TopicPartition(topic, 2)),
        "Partition 2 should have been designated for resume");
    Assert.assertTrue(partitionsToResume.contains(new TopicPartition(topic, 5)),
        "Partition 5 should have been designated for resume");
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
        KafkaConnectorTask.getPartitionsToPause(assignedPartitions, Collections.emptySet(),
            pausedSourcePartitionsConfig, autoPausePartitions);
    Assert.assertEquals(autoPausePartitions, partitionsToPause, "There should have been partitions to pause");

    // auto-resume one of the partitions
    autoPausePartitions.remove(new TopicPartition(topic, 4));
    Set<TopicPartition> partitionsToResume =
        KafkaConnectorTask.getPartitionsToResume(partitionsToPause, pausedSourcePartitionsConfig, autoPausePartitions);
    Assert.assertEquals(partitionsToResume.size(), 1, "There should have been 1 partition to resume");
    Assert.assertTrue(partitionsToResume.contains(new TopicPartition(topic, 4)),
        "Partition 4 should have been designated for resume");
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
        KafkaConnectorTask.getPartitionsToPause(assignedPartitions, Collections.emptySet(),
            pausedSourcePartitionsConfig, autoPausedPartitions);
    Assert.assertEquals(partitionsToPause, IntStream.range(0, 8)
        .filter(i -> i != 4 && i != 6)
        .mapToObj(p -> new TopicPartition(topic, p))
        .collect(Collectors.toSet()));

    // test for resuming an auto-paused partition, by configuring it for pause and then unpausing it
    // resume the auto-paused partition 7
    pausedSourcePartitionsConfig.get(topic).add("7");
    // verify that no partitions should be resumed
    Assert.assertTrue(
        KafkaConnectorTask.getPartitionsToResume(partitionsToPause, pausedSourcePartitionsConfig, autoPausedPartitions)
            .isEmpty(), "No partitions should be designated for resume");
    Set<TopicPartition> newPartitionsToPause =
        KafkaConnectorTask.getPartitionsToPause(assignedPartitions, partitionsToPause, pausedSourcePartitionsConfig,
            autoPausedPartitions);
    Assert.assertTrue(newPartitionsToPause.isEmpty(),
        "There should be no new partitions to pause, but instead found: " + newPartitionsToPause);
    Assert.assertEquals(autoPausedPartitions.size(), 1, "There should only be 1 auto-paused partition");
    Assert.assertTrue(autoPausedPartitions.contains(new TopicPartition(topic, 5)),
        "Only partition 5 should be auto-paused, since partition 7 was moved to config");

    // now resume partition 7, which was auto-paused then configured for pause
    pausedSourcePartitionsConfig.get(topic).remove("7");
    Assert.assertTrue(
        KafkaConnectorTask.getPartitionsToPause(assignedPartitions, partitionsToPause, pausedSourcePartitionsConfig,
            autoPausedPartitions).isEmpty(),
        "There should be no new partitions to pause, but instead found: " + newPartitionsToPause);
    Set<TopicPartition> partitionsToResume =
        KafkaConnectorTask.getPartitionsToResume(partitionsToPause, pausedSourcePartitionsConfig, autoPausedPartitions);
    Assert.assertEquals(partitionsToResume.size(), 1, "There should have been 1 partition to resume");
    Assert.assertTrue(partitionsToResume.contains(new TopicPartition(topic, 7)));
  }
}
