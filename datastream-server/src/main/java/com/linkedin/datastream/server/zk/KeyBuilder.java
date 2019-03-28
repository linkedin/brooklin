/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.server.zk;

public class KeyBuilder {
  private static final String CLUSTER = "/%s";
  private static final String LIVE_INSTANCES = "/%s/liveinstances";
  private static final String LIVE_INSTANCE = "/%s/liveinstances/%s";
  private static final String INSTANCES = "/%s/instances";
  private static final String INSTANCE = "/%s/instances/%s";
  private static final String INSTANCE_ASSIGNMENTS = "/%s/instances/%s/assignments";
  private static final String INSTANCE_ERRORS = "/%s/instances/%s/errors";
  private static final String INSTANCE_ASSIGNMENT = "/%s/instances/%s/assignments/%s";
  private static final String DATASTREAMS = "/%s/dms";
  private static final String DATASTREAM = "/%s/dms/%s";
  private static final String CONNECTORS = "/%s/connectors";
  private static final String CONNECTOR = "/%s/connectors/%s";

  /**
   * There are two ZK nodes for any given DatastreamTask, one under "instances"
   * and the other one under "connectors/connectorType". The former represents
   * an assignment and holds the JSON serialization of the task itself. The one
   * under connectors hold the remaining information of the task, ie. config and
   * state. This separation is because assignment change can be a hot path when
   * there is node up/down, datastream creation/deletion. With the JSON text
   * under "instances", Coordinator only needs to read ZooKeeper once per task,
   * as opposed to a 2nd read into connectors.
   *
   * Below keys represent the various locations under connectors for parts of
   * a DatastreamTask can be persisted.
   */

  /**
   * Task node
   */
  private static final String CONNECTOR_DATASTREAM_TASK = CONNECTOR + "/%s";

  /**
   * Task state node under connectorType/task
   */
  private static final String DATASTREAM_TASK_STATE = CONNECTOR + "/%s/state";

  /**
   * Specific task state node under connectorType/task/state
   */
  private static final String DATASTREAM_TASK_STATE_KEY = CONNECTOR + "/%s/state/%s";

  /**
   * Task config node under connectorType/task
   */
  private static final String DATASTREAM_TASK_CONFIG = CONNECTOR + "/%s/config";

  /**
   * Task lock node under connectorType/task
   */
  private static final String DATASTREAM_TASK_LOCK = CONNECTOR + "/%s/lock";

  public static String cluster(String clusterName) {
    return String.format(CLUSTER, clusterName);
  }

  public static String liveInstances(String cluster) {
    return String.format(LIVE_INSTANCES, cluster);
  }

  public static String liveInstance(String cluster, String instance) {
    return String.format(LIVE_INSTANCE, cluster, instance);
  }

  public static String instances(String cluster) {
    return String.format(INSTANCES, cluster);
  }

  public static String instance(String cluster, String instanceName) {
    return String.format(INSTANCE, cluster, instanceName);
  }

  public static String instanceAssignments(String cluster, String instance) {
    return String.format(INSTANCE_ASSIGNMENTS, cluster, instance);
  }

  public static String instanceErrors(String cluster, String instance) {
    return String.format(INSTANCE_ERRORS, cluster, instance);
  }

  public static String instanceAssignment(String cluster, String instance, String name) {
    return String.format(INSTANCE_ASSIGNMENT, cluster, instance, name);
  }

  public static String datastreams(String cluster) {
    return String.format(DATASTREAMS, cluster);
  }

  public static String datastream(String cluster, String stream) {
    return String.format(DATASTREAM, cluster, stream);
  }

  public static String connector(String cluster, String connectorType) {
    return String.format(CONNECTOR, cluster, connectorType);
  }

  public static String connectors(String cluster) {
    return String.format(CONNECTORS, cluster);
  }

  public static String connectorTask(String cluster, String connectorType, String name) {
    return String.format(CONNECTOR_DATASTREAM_TASK, cluster, connectorType, name);
  }

  // ZooKeeper path: /{cluster}/connectors/{connectorType}/{taskName}/state
  public static String datastreamTaskState(String cluster, String connectorType, String name) {
    // taskId could be empty space, which can result in "//" in the path
    return String.format(DATASTREAM_TASK_STATE, cluster, connectorType, name).replaceAll("//", "/");
  }

  // ZooKeeper path: /{cluster}/connectors/{connectorType}/{taskName}/config
  public static String datastreamTaskConfig(String cluster, String connectorType, String name) {
    return String.format(DATASTREAM_TASK_CONFIG, cluster, connectorType, name).replaceAll("//", "/");
  }

  // ZooKeeper path: /{cluster}/connectors/{connectorType}/{taskName}/state
  public static String datastreamTaskStateKey(String cluster, String connectorType, String name, String key) {
    // taskId could be empty space, which can result in "//" in the path
    return String.format(DATASTREAM_TASK_STATE_KEY, cluster, connectorType, name, key).replaceAll("//", "/");
  }

  // ZooKeeper path: /{cluster}/connectors/{connectorType}/{taskName}/config
  public static String datastreamTaskLock(String cluster, String connectorType, String name) {
    return String.format(DATASTREAM_TASK_LOCK, cluster, connectorType, name).replaceAll("//", "/");
  }
}
