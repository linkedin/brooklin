/**
 *  Copyright 2021 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.server.assignment;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;

import com.linkedin.datastream.common.DatastreamRuntimeException;
import com.linkedin.datastream.common.JsonUtils;
import com.linkedin.datastream.metrics.BrooklinGaugeInfo;
import com.linkedin.datastream.metrics.BrooklinMetricInfo;
import com.linkedin.datastream.metrics.DynamicMetricsManager;
import com.linkedin.datastream.metrics.MetricsAware;
import com.linkedin.datastream.server.ClusterThroughputInfo;
import com.linkedin.datastream.server.DatastreamGroupPartitionsMetadata;
import com.linkedin.datastream.server.DatastreamTask;
import com.linkedin.datastream.server.DatastreamTaskImpl;
import com.linkedin.datastream.server.PartitionThroughputInfo;


/**
 * Performs partition assignment based on partition throughput information
 */
public class LoadBasedPartitionAssigner implements MetricsAware {
  private static final Logger LOG = LoggerFactory.getLogger(LoadBasedPartitionAssigner.class.getName());
  private static final String CLASS_NAME = LoadBasedPartitionAssigner.class.getSimpleName();
  private static final DynamicMetricsManager DYNAMIC_METRICS_MANAGER = DynamicMetricsManager.getInstance();
  private static final String MIN_PARTITIONS_ACROSS_TASKS = "minPartitionsAcrossTasks";
  private static final String MAX_PARTITIONS_ACROSS_TASKS = "maxPartitionsAcrossTasks";

  private final Map<String, LoadBasedPartitionAssigner.PartitionAssignmentStats> _partitionAssignmentStatsMap =
      new ConcurrentHashMap<>();
  /**
   * Performs partition assignment based on partition throughput information.
   * <p>
   * Unlike other assignment algorithms, this one can result in uneven distribution of partitions in tasks. Partitions
   * with no throughput information (such as newly discovered partitions) will be assigned to tasks using round-robin.
   * </p>
   * @param throughputInfo Per partition throughput information
   * @param currentAssignment Current assignment
   * @param unassignedPartitions Unassigned partitions
   * @param partitionMetadata Partition metadata
   * @return New assignment
   */
  public Map<String, Set<DatastreamTask>> assignPartitions(
      ClusterThroughputInfo throughputInfo, Map<String, Set<DatastreamTask>> currentAssignment,
      List<String> unassignedPartitions, DatastreamGroupPartitionsMetadata partitionMetadata, int maxPartitionsPerTask) {
    String datastreamGroupName = partitionMetadata.getDatastreamGroup().getName();
    Map<String, PartitionThroughputInfo> partitionInfoMap = throughputInfo.getPartitionInfoMap();
    Set<String> tasksWithChangedPartition = new HashSet<>();

    // filter out all the tasks for the current datastream group, and retain assignments in a map
    Map<String, Set<String>> newPartitions = new HashMap<>();
    currentAssignment.values().forEach(tasks ->
        tasks.forEach(task -> {
          if (task.getTaskPrefix().equals(datastreamGroupName)) {
            Set<String> retainedPartitions = new HashSet<>(task.getPartitionsV2());
            retainedPartitions.retainAll(partitionMetadata.getPartitions());
            newPartitions.put(task.getId(), retainedPartitions);
            if (retainedPartitions.size() != task.getPartitionsV2().size()) {
              tasksWithChangedPartition.add(task.getId());
            }
          }
    }));

    int numPartitions = partitionMetadata.getPartitions().size();
    int numTasks = newPartitions.size();
    validatePartitionCountAndThrow(datastreamGroupName, numTasks, numPartitions, maxPartitionsPerTask);

    // sort the current assignment's tasks on total throughput
    Map<String, Integer> taskThroughputMap = new HashMap<>();
    PartitionThroughputInfo defaultPartitionInfo = new PartitionThroughputInfo(
        LoadBasedPartitionAssignmentStrategyConfig.DEFAULT_PARTITION_BYTES_IN_KB_RATE,
        LoadBasedPartitionAssignmentStrategyConfig.DEFAULT_PARTITION_MESSAGES_IN_RATE, "");
    newPartitions.forEach((task, partitions) -> {
      int totalThroughput = partitions.stream()
          .mapToInt(p -> partitionInfoMap.getOrDefault(p, defaultPartitionInfo).getBytesInKBRate())
          .sum();
      taskThroughputMap.put(task, totalThroughput);
    });

    ArrayList<String> recognizedPartitions = new ArrayList<>(); // partitions with throughput info
    ArrayList<String> unrecognizedPartitions = new ArrayList<>(); // partitions without throughput info
    for (String partition : unassignedPartitions) {
      if (partitionInfoMap.containsKey(partition)) {
        recognizedPartitions.add(partition);
      } else {
        unrecognizedPartitions.add(partition);
      }
    }

    // sort unassigned partitions with throughput info on throughput
    recognizedPartitions.sort((p1, p2) -> {
      Integer p1KBRate = partitionInfoMap.get(p1).getBytesInKBRate();
      Integer p2KBRate = partitionInfoMap.get(p2).getBytesInKBRate();
      return p1KBRate.compareTo(p2KBRate);
    });

    // build a priority queue of tasks based on throughput
    // only add tasks that can accommodate more partitions in the queue
    List<String> tasks = newPartitions.keySet().stream()
        .filter(t -> newPartitions.get(t).size() < maxPartitionsPerTask)
        .collect(Collectors.toList());
    PriorityQueue<String> taskQueue = new PriorityQueue<>(Comparator.comparing(taskThroughputMap::get));
    taskQueue.addAll(tasks);

    // assign partitions with throughput info one by one, by putting the heaviest partition in the lightest task
    while (recognizedPartitions.size() > 0 && taskQueue.size() > 0) {
       String heaviestPartition = recognizedPartitions.remove(recognizedPartitions.size() - 1);
       int heaviestPartitionThroughput = partitionInfoMap.get(heaviestPartition).getBytesInKBRate();
       String lightestTask = taskQueue.poll();
       newPartitions.get(lightestTask).add(heaviestPartition);
       taskThroughputMap.put(lightestTask, taskThroughputMap.get(lightestTask) + heaviestPartitionThroughput);
       tasksWithChangedPartition.add(lightestTask);
       int currentNumPartitions = newPartitions.get(lightestTask).size();
       // don't put the task back in the queue if the number of its partitions is maxed out
       if (currentNumPartitions < maxPartitionsPerTask) {
         taskQueue.add(lightestTask);
       }
    }

    // assign unrecognized partitions with round-robin
    Collections.shuffle(unrecognizedPartitions);
    int index = 0;
    for (String partition : unrecognizedPartitions) {
      index = findTaskWithRoomForAPartition(tasks, newPartitions, index, maxPartitionsPerTask);
      String currentTask = tasks.get(index);
      newPartitions.get(currentTask).add(partition);
      tasksWithChangedPartition.add(currentTask);
      index = (index + 1) % tasks.size();
    }

    AtomicInteger minPartitionsAcrossTasks = new AtomicInteger(Integer.MAX_VALUE);
    AtomicInteger maxPartitionsAcrossTasks = new AtomicInteger(0);
    // build the new assignment using the new partitions for the affected datastream's tasks
    Map<String, Set<DatastreamTask>> newAssignments = new HashMap<>();
    currentAssignment.keySet().forEach(instance -> {
      Set<DatastreamTask> oldTasks = currentAssignment.get(instance);
      Set<DatastreamTask> newTasks = oldTasks.stream()
          .map(task -> {
            int partitionCount = newPartitions.containsKey(task.getId()) ? newPartitions.get(task.getId()).size() :
                task.getPartitionsV2().size();

            minPartitionsAcrossTasks.set(Math.min(minPartitionsAcrossTasks.get(), partitionCount));
            maxPartitionsAcrossTasks.set(Math.max(maxPartitionsAcrossTasks.get(), partitionCount));
            if (tasksWithChangedPartition.contains(task.getId())) {
              DatastreamTaskImpl newTask = new DatastreamTaskImpl((DatastreamTaskImpl) task, newPartitions.get(task.getId()));
              /*String stats = String.format("{\'throughput\':%d, \'numPartitions\':%d, \'partitionsWithUnknownThroughputRate\': %d}",
                  throughputRate, partitionCount, partitionCount);
              task.setStats(stats);//saveState("throughputMetadata", throughputRate.toString()); */
              PartitionAssignmentStatPerTask stat = PartitionAssignmentStatPerTask.fromJson(((DatastreamTaskImpl) task).getStats());
              if (partitionInfoMap.isEmpty()) {
                stat.isThroughputRateLatest = false;
              } else {
                stat.throughputRate = taskThroughputMap.get(task.getId());
                stat.isThroughputRateLatest = true;
              }
              stat.totalPartitions = partitionCount;
              //TODO: Update this metric.
              stat.partitionsWithUnknownThroughput = 0;
              try {
                newTask.setStats(stat.toJson());
              } catch (IOException e) {
                LOG.error("Exception while saving the stats to Json", e);
              }
              return newTask;
            }
            return task;
          }).collect(Collectors.toSet());
      newAssignments.put(instance, newTasks);
    });

    // update metrics
    PartitionAssignmentStats stats = new PartitionAssignmentStats(minPartitionsAcrossTasks.get(),
        maxPartitionsAcrossTasks.get());
    String taskPrefix = partitionMetadata.getDatastreamGroup().getTaskPrefix();
    updateMetricsForDatastream(taskPrefix, stats);
    LOG.info("Assignment stats for {}. Min partitions across tasks: {}, max partitions across tasks: {}", taskPrefix,
        stats.getMinPartitionsAcrossTasks(), stats.getMaxPartitionsAcrossTasks());

    return newAssignments;
  }

  private void validatePartitionCountAndThrow(String datastream, int numTasks, int numPartitions,
      int maxPartitionsPerTask) {
    // conversion to long to avoid integer overflow
    if (numTasks * (long) maxPartitionsPerTask < numPartitions) {
      String message = String.format("Not enough tasks to fit partitions. Datastream: %s, Number of tasks: %d, " +
          "number of partitions: %d, max partitions per task: %d", datastream, numTasks, numPartitions,
          maxPartitionsPerTask);
      throw new DatastreamRuntimeException(message);
    }
  }

  @VisibleForTesting
  int findTaskWithRoomForAPartition(List<String> tasks, Map<String, Set<String>> partitionMap, int startIndex,
      int maxPartitionsPerTask) {
    for (int i = 0; i < tasks.size(); i++) {
      int currentIndex = (startIndex + i) % tasks.size();
      String currentTask = tasks.get(currentIndex);
      if (partitionMap.get(currentTask).size() < maxPartitionsPerTask) {
        return currentIndex;
      }
    }
    throw new DatastreamRuntimeException("No tasks found that can host an additional partition");
  }

  void updateMetricsForDatastream(String datastream, PartitionAssignmentStats stats) {
    if (!_partitionAssignmentStatsMap.containsKey(datastream)) {
      registerLoadBasedPartitionAssignmentMetrics(datastream);
    }
    _partitionAssignmentStatsMap.put(datastream, stats);
  }

  private void registerLoadBasedPartitionAssignmentMetrics(String datastream) {
    Supplier<Integer> minPartitionsAcrossTasksSupplier = () -> _partitionAssignmentStatsMap
        .getOrDefault(datastream, PartitionAssignmentStats.DEFAULT).getMinPartitionsAcrossTasks();
    DYNAMIC_METRICS_MANAGER.registerGauge(CLASS_NAME, datastream, MIN_PARTITIONS_ACROSS_TASKS,
        minPartitionsAcrossTasksSupplier);

    Supplier<Integer> maxPartitionsAcrossTasksSupplier = () -> _partitionAssignmentStatsMap
        .getOrDefault(datastream, PartitionAssignmentStats.DEFAULT).getMaxPartitionsAcrossTasks();
    DYNAMIC_METRICS_MANAGER.registerGauge(CLASS_NAME, datastream, MAX_PARTITIONS_ACROSS_TASKS,
        maxPartitionsAcrossTasksSupplier);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public List<BrooklinMetricInfo> getMetricInfos() {
    List<BrooklinMetricInfo> metricInfos = new ArrayList<>();
    String prefix = CLASS_NAME + MetricsAware.KEY_REGEX;

    metricInfos.add(new BrooklinGaugeInfo(prefix + MIN_PARTITIONS_ACROSS_TASKS));
    metricInfos.add(new BrooklinGaugeInfo(prefix + MAX_PARTITIONS_ACROSS_TASKS));

    return Collections.unmodifiableList(metricInfos);
  }

  void cleanupMetrics() {
    _partitionAssignmentStatsMap.keySet().forEach(this::unregisterMetricsForDatastream);
    _partitionAssignmentStatsMap.clear();
  }

  void unregisterMetricsForDatastream(String datastream) {
    DYNAMIC_METRICS_MANAGER.unregisterMetric(CLASS_NAME, datastream, MIN_PARTITIONS_ACROSS_TASKS);
    DYNAMIC_METRICS_MANAGER.unregisterMetric(CLASS_NAME, datastream, MAX_PARTITIONS_ACROSS_TASKS);
  }

  private static class PartitionAssignmentStatPerTask {
    private int throughputRate;
    private int totalPartitions;
    private int partitionsWithUnknownThroughput;
    private boolean isThroughputRateLatest;

    //getters and setters required for fromJson and toJson
    public int getThroughputRate() {
      return throughputRate;
    }

    public void setThroughputRate(int throughputRate) {
      this.throughputRate = throughputRate;
    }

    public int getTotalPartitions() {
      return totalPartitions;
    }

    public void setTotalPartitions(int totalPartitions) {
      this.totalPartitions = totalPartitions;
    }

    public int getPartitionsWithUnknownThroughput() {
      return partitionsWithUnknownThroughput;
    }

    public void setPartitionsWithUnknownThroughput(int partitionsWithUnknownThroughput) {
      this.partitionsWithUnknownThroughput = partitionsWithUnknownThroughput;
    }

    public boolean getIsThroughputRateLatest() {
      return isThroughputRateLatest;
    }

    public void setIsThroughputRateLatest(boolean isThroughputRateLatest) {
      this.isThroughputRateLatest = isThroughputRateLatest;
    }

    /**
     * Construct PartitionAssignmentStatPerTask from json string
     * @param  json JSON string of the PartitionAssignmentStatPerTask
     */
    public static PartitionAssignmentStatPerTask fromJson(String json) {
      PartitionAssignmentStatPerTask stat = new PartitionAssignmentStatPerTask();
      if (StringUtils.isNotEmpty(json)) {
        stat = JsonUtils.fromJson(json, PartitionAssignmentStatPerTask.class);
      }
      LOG.info("Loaded existing PartitionAssignmentStatPerTask: {}", stat);
      return stat;
    }

    /**
     * Get PartitionAssignmentStatPerTask serialized as JSON
     */
    public String toJson() throws IOException {
      return JsonUtils.toJson(this);
    }

  }
  /**
   * Encapsulates assignment metrics for a single datastream group
   */
  private static class PartitionAssignmentStats {
    private final int _minPartitionsAcrossTasks;
    private final int _maxPartitionsAcrossTasks;

    public static final PartitionAssignmentStats DEFAULT = new PartitionAssignmentStats(0, 0);

    /**
     * Creates an instance of {@link PartitionAssignmentStats}
     * @param minPartitionsAcrossTasks Minimum number of partitions across tasks
     * @param maxPartitionsAcrossTasks Maximum number of partitions across tasks
     */
    public PartitionAssignmentStats(int minPartitionsAcrossTasks, int maxPartitionsAcrossTasks) {
      _minPartitionsAcrossTasks = minPartitionsAcrossTasks;
      _maxPartitionsAcrossTasks = maxPartitionsAcrossTasks;
    }

    /**
     * Gets the minimum number of partitions across tasks
     * @return Minimum number of partitions across tasks
     */
    public int getMinPartitionsAcrossTasks() {
      return _minPartitionsAcrossTasks;
    }

    /**
     * Gets the maximum number of partitions across tasks
     * @return Maximum number of partitions across tasks
     */
    public int getMaxPartitionsAcrossTasks() {
      return _maxPartitionsAcrossTasks;
    }
  }
}
