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
import java.util.IntSummaryStatistics;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Gauge;

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

  private final int _defaultPartitionBytesInKBRate;
  private final int _defaultPartitionMsgsInRate;

  private final Map<String, DatastreamMetrics> _metricsForDatastream = new ConcurrentHashMap<>();

  /**
   * Constructor of LoadBasedPartitionAssigner
   * @param defaultPartitionBytesInKBRate default bytesIn rate in KB for partition
   * @param defaultPartitionMsgsInRate default msgsIn rate in KB for partition
   */
  public LoadBasedPartitionAssigner(int defaultPartitionBytesInKBRate, int defaultPartitionMsgsInRate) {
    _defaultPartitionBytesInKBRate = defaultPartitionBytesInKBRate;
    _defaultPartitionMsgsInRate = defaultPartitionMsgsInRate;
  }

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

    Map<String, PartitionThroughputInfo> partitionInfoMap = new HashMap<>(throughputInfo.getPartitionInfoMap());
    PartitionThroughputInfo defaultPartitionInfo = new PartitionThroughputInfo(_defaultPartitionBytesInKBRate,
        _defaultPartitionMsgsInRate, "");

    // filter out all the tasks for the current datastream group, and retain assignments in a map
    Assignments assignments = currentAssignment.values()
        .stream()
        .parallel()
        .flatMap(tasks -> tasks.stream()
            .parallel()
            .filter(task -> task.getTaskPrefix().equals(datastreamGroupName))
            .map(task -> {
              Set<String> retainedPartitions = new HashSet<>(task.getPartitionsV2());
              retainedPartitions.retainAll(partitionMetadata.getPartitions());

              int throughput = retainedPartitions.stream().parallel().mapToInt(p -> {
                String topic = extractTopicFromPartition(p);
                PartitionThroughputInfo defaultValue = partitionInfoMap.getOrDefault(topic, defaultPartitionInfo);
                return partitionInfoMap.getOrDefault(p, defaultValue).getBytesInKBRate();
              }).sum();

              boolean changed = retainedPartitions.size() != task.getPartitionsV2().size();

              return new Assignment(task.getId(), retainedPartitions, changed, throughput);
            }))
        .collect(() -> new Assignments(maxPartitionsPerTask), Assignments::add, Assignments::addAll);

    // sort the current assignment's tasks on total throughput
    Map<String, Set<String>> newPartitionAssignmentMap = assignments.assignments;
    Set<String> tasksWithChangedPartition = assignments.modified;
    Map<String, Integer> taskThroughputMap = assignments.throughput;

    int numPartitions = partitionMetadata.getPartitions().size();
    int numTasks = newPartitionAssignmentMap.size();
    validatePartitionCountAndThrow(datastreamGroupName, numTasks, numPartitions, maxPartitionsPerTask);

    ArrayList<String> recognizedPartitions = new ArrayList<>(); // partitions with throughput info
    ArrayList<String> unrecognizedPartitions = new ArrayList<>(); // partitions without throughput info
    for (String partition : unassignedPartitions) {
      if (partitionInfoMap.containsKey(partition)) {
        recognizedPartitions.add(partition);
      } else {
        // If the partition level information is not found, try finding topic level information. It is always better
        // than no information about the partition. Update the map with that information so that it can be used in later
        // part of the code.
        String topic = extractTopicFromPartition(partition);
        if (partitionInfoMap.containsKey(topic)) {
          partitionInfoMap.put(partition, partitionInfoMap.get(topic));
          recognizedPartitions.add(partition);
        } else {
          unrecognizedPartitions.add(partition);
        }
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
    PriorityQueue<String> taskQueue = new PriorityQueue<>(Comparator.comparing(taskThroughputMap::get));
    taskQueue.addAll(assignments.belowCapacity);
    ArrayList<String> tasks = new ArrayList<>(taskQueue);

    // assign partitions with throughput info one by one, by putting the heaviest partition in the lightest task
    while (recognizedPartitions.size() > 0 && taskQueue.size() > 0) {
       String heaviestPartition = recognizedPartitions.remove(recognizedPartitions.size() - 1);
       int heaviestPartitionThroughput = partitionInfoMap.get(heaviestPartition).getBytesInKBRate();
       String lightestTask = taskQueue.poll();
       newPartitionAssignmentMap.get(lightestTask).add(heaviestPartition);
       taskThroughputMap.put(lightestTask, taskThroughputMap.get(lightestTask) + heaviestPartitionThroughput);
       tasksWithChangedPartition.add(lightestTask);
       int currentNumPartitions = newPartitionAssignmentMap.get(lightestTask).size();
       // don't put the task back in the queue if the number of its partitions is maxed out
       if (currentNumPartitions < maxPartitionsPerTask) {
         taskQueue.add(lightestTask);
       }
    }

    // assign unrecognized partitions with round-robin
    Map<String, Integer> unrecognizedPartitionCountPerTask = new HashMap<>();
    Collections.shuffle(unrecognizedPartitions);
    int index = 0;
    for (String partition : unrecognizedPartitions) {
      index = findTaskWithRoomForAPartition(tasks, newPartitionAssignmentMap, index, maxPartitionsPerTask);
      String currentTask = tasks.get(index);
      newPartitionAssignmentMap.get(currentTask).add(partition);
      tasksWithChangedPartition.add(currentTask);
      index = (index + 1) % tasks.size();
      unrecognizedPartitionCountPerTask.put(currentTask, unrecognizedPartitionCountPerTask.getOrDefault(currentTask, 0) + 1);
    }

    // build the new assignment using the new partitions for the affected datastream's tasks
    Map<String, Set<DatastreamTask>> newAssignments = currentAssignment.entrySet().stream()
      .collect(Collectors.toMap(x -> x.getKey(), x -> x.getValue().stream()
        .map(task -> {
          if (tasksWithChangedPartition.contains(task.getId())) {
            Set<String> newAssignment = newPartitionAssignmentMap.get(task.getId());
            DatastreamTaskImpl newTask = new DatastreamTaskImpl((DatastreamTaskImpl) task, newAssignment);
            saveStats(partitionInfoMap, taskThroughputMap, unrecognizedPartitionCountPerTask, task, newAssignment.size(), newTask);
            return newTask;
          } else {
            return task;
          }
        })
        .collect(Collectors.toSet())));

    IntSummaryStatistics stats = newAssignments.values().stream()
      .flatMap(x -> x.stream()) // flatten
      .filter(x -> x.getTaskPrefix().equals(datastreamGroupName))
      .collect(Collectors.summarizingInt(x -> x.getPartitionsV2().size()));

    // update metrics
    String taskPrefix = partitionMetadata.getDatastreamGroup().getTaskPrefix();
    DatastreamMetrics metrics = metricsForDatastream(taskPrefix);
    metrics.minPartitionsAcrossTasks(stats.getMin());
    metrics.maxPartitionsAcrossTasks(stats.getMax());
    LOG.info("Assignment stats for {}. Min partitions across tasks: {}, max partitions across tasks: {}", taskPrefix,
        stats.getMin(), stats.getMax());

    return newAssignments;
  }

  /**
   * A {@link java.util.function.Consumer} that accumulates {@link Assignment}s.
   */
  private static class Assignments {
    final Map<String, Set<String>> assignments = new HashMap<>();
    final Map<String, Integer> throughput = new HashMap<>();
    final Set<String> belowCapacity = new HashSet<>();
    final Set<String> modified = new HashSet<>();
    private final int overload;

    Assignments(int overload) {
      this.overload = overload;
    }

    static void add(Assignments assignments, Assignment assignment) {
      assignments.assignments.put(assignment.taskId, assignment.partitions);
      assignments.throughput.put(assignment.taskId, assignment.throughput);

      if (assignment.partitions.size() < assignments.overload) {
        assignments.belowCapacity.add(assignment.taskId);
      }

      if (assignment.changed) {
        assignments.modified.add(assignment.taskId);
      }
    }

    static void addAll(Assignments left, Assignments right) {
      left.assignments.putAll(right.assignments);
      left.throughput.putAll(right.throughput);

      left.belowCapacity.addAll(right.belowCapacity);
      left.modified.addAll(right.modified);
    }
  }

  private static class Assignment {
    final String taskId;
    final Set<String> partitions;
    final boolean changed;
    final int throughput;

    Assignment(String taskId, Set<String> partitions, boolean changed, int throughput) {
      this.taskId = taskId;
      this.partitions = partitions;
      this.changed = changed;
      this.throughput = throughput;
    }
  }

  private DatastreamMetrics metricsForDatastream(String taskPrefix) {
    return _metricsForDatastream.computeIfAbsent(taskPrefix, (x) -> new DatastreamMetrics(x));
  }

  private void saveStats(Map<String, PartitionThroughputInfo> partitionInfoMap, Map<String, Integer> taskThroughputMap,
      Map<String, Integer> unrecognizedPartitionCountPerTask, DatastreamTask task, int partitionCount,
      DatastreamTaskImpl newTask) {
    PartitionAssignmentStatPerTask stat = PartitionAssignmentStatPerTask.fromJson(((DatastreamTaskImpl) task).getStats());
    if (partitionInfoMap.isEmpty()) {
      stat.isThroughputRateLatest = false;
    } else {
      stat.throughputRateInKBps = taskThroughputMap.get(task.getId());
      stat.isThroughputRateLatest = true;
    }
    stat.totalPartitions = partitionCount;
    // ignores the partitions removed. This value will be approximate.
    stat.partitionsWithUnknownThroughput += unrecognizedPartitionCountPerTask.getOrDefault(task.getId(), 0);
    try {
      newTask.setStats(stat.toJson());
    } catch (IOException e) {
      LOG.error("Exception while saving the stats to Json for task {}", task.getId(), e);
    }
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
    _metricsForDatastream.keySet().forEach(this::unregisterMetricsForDatastream);
  }

  void unregisterMetricsForDatastream(String datastream) {
    // cleanup existing DatastreamMetrics object, then remove it
    _metricsForDatastream.compute(datastream, (k, v) -> {
      if (v != null) {
        v.cleanup();
      }
      return null;
    });
  }

  /**
   *
   * @param partition partition name
   * @return topic name
   */
  static String extractTopicFromPartition(String partition) {
    String topic = partition;
    int index = partition.lastIndexOf('-');
    if (index > -1) {
      topic = partition.substring(0, index);
    }
    return topic;
  }

  static class PartitionAssignmentStatPerTask {
    private int throughputRateInKBps;
    private int totalPartitions;
    private int partitionsWithUnknownThroughput;
    private boolean isThroughputRateLatest;

    //getters and setters required for fromJson and toJson
    public int getThroughputRateInKBps() {
      return throughputRateInKBps;
    }

    public void setThroughputRateInKBps(int throughputRateInKBps) {
      this.throughputRateInKBps = throughputRateInKBps;
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

  private static class DatastreamMetrics {
    private final String taskPrefix;
    private final Gauge<Integer> minPartitionsAcrossTasks;
    private final Gauge<Integer> maxPartitionsAcrossTasks;

    DatastreamMetrics(String taskPrefix) {
      this.taskPrefix = taskPrefix;
      minPartitionsAcrossTasks = DYNAMIC_METRICS_MANAGER.registerGauge(CLASS_NAME, taskPrefix,
          MIN_PARTITIONS_ACROSS_TASKS, () -> 0);
      maxPartitionsAcrossTasks = DYNAMIC_METRICS_MANAGER.registerGauge(CLASS_NAME, taskPrefix,
          MAX_PARTITIONS_ACROSS_TASKS, () -> 0);
    }

    void cleanup() {
      DYNAMIC_METRICS_MANAGER.unregisterMetric(CLASS_NAME, taskPrefix, MIN_PARTITIONS_ACROSS_TASKS);
      DYNAMIC_METRICS_MANAGER.unregisterMetric(CLASS_NAME, taskPrefix, MAX_PARTITIONS_ACROSS_TASKS);
    }

    void minPartitionsAcrossTasks(int min) {
      DYNAMIC_METRICS_MANAGER.setGauge(minPartitionsAcrossTasks, () -> min);
    }

    void maxPartitionsAcrossTasks(int max) {
      DYNAMIC_METRICS_MANAGER.setGauge(maxPartitionsAcrossTasks, () -> max);
    }
  }
}
