/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.server.zk;

/**
 * Helper class to build commonly accessed ZooKeeper znodes
 */
public final class KeyBuilder {
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

  // Suppresses default constructor, ensuring non-instantiability.
  private KeyBuilder() {
  }

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

   * Task node under connectorType
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
   * Lock node
   */
  static final String DATASTREAM_TASK_LOCK_ROOT_NAME = "lock";

  /**
   * Task lock root path under connectorType
   */
  private static final String DATASTREAM_TASK_LOCK_ROOT = CONNECTOR + "/" + DATASTREAM_TASK_LOCK_ROOT_NAME;

  /**
   * Task lock node under connectorType/lock/{taskPrefix}
   */
  private static final String DATASTREAM_TASK_LOCK_PREFIX = DATASTREAM_TASK_LOCK_ROOT + "/%s";

  /**
   * Task lock node under connectorType/lock/{taskPrefix}/{task}
   */
  private static final String DATASTREAM_TASK_LOCK = DATASTREAM_TASK_LOCK_PREFIX + "/%s";

  /**
   * Base path to store partition movement info
   */
  private static final String TARGET_ASSIGNMENT_BASE = CONNECTOR + "/targetAssignment";

  /**
   * partition movement info under connectorType/targetAssignment/datastreamGroup
   */
  private static final String TARGET_ASSIGNMENTS = TARGET_ASSIGNMENT_BASE + "/%s";

  /**
   * Get the root level ZooKeeper znode of a Brooklin cluster
   * @param clusterName Brooklin cluster name
   */
  public static String cluster(String clusterName) {
    return String.format(CLUSTER, clusterName);
  }

  /**
   * Get the ZooKeeper znode containing the list of live instances participating in a Brooklin cluster
   *
   * The /liveinstances znode  is where the different Brooklin instances create ephemeral znodes with incremental
   * sequence numbers for the purposes of leader Coordinator election.
   * @param cluster Brooklin cluster name
   * @see #liveInstance(String, String)
   * @see #instances(String)
   */
  public static String liveInstances(String cluster) {
    return String.format(LIVE_INSTANCES, cluster);
  }

  /**
   * Get the ZooKeeper znode for a specific live instance participating in a Brooklin cluster
   * @param cluster Brooklin cluster name
   * @param instance Live instance name
   * @see #liveInstances(String)
   */
  public static String liveInstance(String cluster, String instance) {
    return String.format(LIVE_INSTANCE, cluster, instance);
  }

  /**
   * Get the ZooKeeper znode containing the list of instances participating in a Brooklin cluster.
   *
   * The <i>/{cluster}/instances</i> znode is where every Brooklin instance creates a persistent znode for itself.
   * Each znode has a name composed of the concatenation of the instance hostname and its unique sequence number under
   * <i>/{cluster}/liveinstances</i>.
   * @param cluster Brooklin cluster name
   * @see #instance(String, String)
   * @see #liveInstances(String)
   */
  public static String instances(String cluster) {
    return String.format(INSTANCES, cluster);
  }

  /**
   * Get the ZooKeeper znode for a specific instance participating in a Brooklin cluster
   * @param cluster Brooklin cluster name
   * @param instanceName Instance name
   */
  public static String instance(String cluster, String instanceName) {
    return String.format(INSTANCE, cluster, instanceName);
  }

  /**
   * Get the ZooKeeper znode containing the list of datastream task assignments for a specific instance
   * @param cluster Brooklin cluster name
   * @param instance Instance name
   */
  public static String instanceAssignments(String cluster, String instance) {
    return String.format(INSTANCE_ASSIGNMENTS, cluster, instance);
  }

  /**
   * Get the ZooKeeper znode that persists messages about errors encountered by a Brooklin instance
   * @param cluster Brooklin cluster name
   * @param instance Instance name
   */
  public static String instanceErrors(String cluster, String instance) {
    return String.format(INSTANCE_ERRORS, cluster, instance);
  }

  /**
   * Get the ZooKeeper znode for a specific datastream task assigned to a Brooklin instance
   *
   * This znode holds the JSON serialization of the task itself.
   * @param cluster Brooklin cluster name
   * @param instance Instance name
   * @param datastreamTask Datastream task name
   * @see #connectorTask(String, String, String)
   */
  public static String instanceAssignment(String cluster, String instance, String datastreamTask) {
    return String.format(INSTANCE_ASSIGNMENT, cluster, instance, datastreamTask);
  }

  /**
   * Get the ZooKeeper znode containing all datastreams in a Brooklin cluster
   * @param cluster Brooklin cluster name
   */
  public static String datastreams(String cluster) {
    return String.format(DATASTREAMS, cluster);
  }

  /**
   * Get the ZooKeeper znode for a specific datastream in a Brooklin cluster
   * @param cluster Brooklin cluster name
   * @param stream Datastream name
   */
  public static String datastream(String cluster, String stream) {
    return String.format(DATASTREAM, cluster, stream);
  }

  /**
   * Get the ZooKeeper znode for a specific connector enabled in a Brooklin cluster
   * @param cluster Brooklin cluster name
   * @param connectorType Connector
   */
  public static String connector(String cluster, String connectorType) {
    return String.format(CONNECTOR, cluster, connectorType);
  }

  /**
   * Get the ZooKeeper znode containing the list of all enabled connectors in a Brooklin cluster
   * @param cluster Brooklin cluster name
   */
  public static String connectors(String cluster) {
    return String.format(CONNECTORS, cluster);
  }

  /**
   * Get the ZooKeeper znode for a specific datastream task under a connector znode
   *
   * This node holds the config and state information of the task as opposed to the task
   * node under the /instances znode which has the JSON serialization of the task itself.
   * @param cluster Brooklin cluster name
   * @param connectorType Connector
   * @param datastreamTask Datastream task name
   * @see #instanceAssignment(String, String, String)
   */
  public static String connectorTask(String cluster, String connectorType, String datastreamTask) {
    return String.format(CONNECTOR_DATASTREAM_TASK, cluster, connectorType, datastreamTask);
  }

  /**
   * Get the ZooKeeper znode for the state of a specific datastream task under a connector znode
   *
   * <pre>Example: /{cluster}/connectors/{connectorType}/{taskName}/state</pre>
   *
   * The state stores the current state including progress information in the form of offsets/checkpoints
   * or error messages in case of errors.
   * @param cluster Brooklin cluster name
   * @param connectorType Connector
   * @param datastreamTask Datastream task name
   */
  public static String datastreamTaskState(String cluster, String connectorType, String datastreamTask) {
    // taskId could be empty space, which can result in "//" in the path
    return String.format(DATASTREAM_TASK_STATE, cluster, connectorType, datastreamTask).replaceAll("//", "/");
  }

  /**
   * Get the ZooKeeper znode for the configurations of a specific datastream task under a connector znode
   *
   * <pre>Example: /{cluster}/connectors/{connectorType}/{taskName}/config</pre>
   *
   * @param cluster Brooklin cluster name
   * @param connectorType Connector
   * @param datastreamTask Datastream task name
   */
  public static String datastreamTaskConfig(String cluster, String connectorType, String datastreamTask) {
    return String.format(DATASTREAM_TASK_CONFIG, cluster, connectorType, datastreamTask).replaceAll("//", "/");
  }

  /**
   * Get the ZooKeeper znode for a specific type of state information of a datastream task under a connector znode
   *
   * <pre>Example: /{cluster}/connectors/{connectorType}/{taskName}/state</pre>
   *
   * An example of a specific type of state is the sourceCheckpoint. Different connectors can persist various state
   * categories of information they deem valuable under different keys.
   * @param cluster Brooklin cluster name
   * @param connectorType Connector
   * @param datastreamTask Datastream task name
   * @param key Key used to store the specific category of state information
   */
  public static String datastreamTaskStateKey(String cluster, String connectorType, String datastreamTask, String key) {
    // taskId could be empty space, which can result in "//" in the path
    return String.format(DATASTREAM_TASK_STATE_KEY, cluster, connectorType, datastreamTask, key).replaceAll("//", "/");
  }


  /**
   * Get the ZooKeeper path to store lock
   *
   * <pre>Example: /{cluster}/connectors/{connectorType}/lock</pre>
   * @param cluster Brooklin cluster name
   * @param connectorType Connector
   * @return datastream task lock root
   */
  public static String datastreamTaskLockRoot(String cluster, String connectorType) {
    return String.format(DATASTREAM_TASK_LOCK_ROOT, cluster, connectorType).replaceAll("//", "/");
  }

  /**
   * Get the ZooKeeper znode for a specific datastream task's lock prefix
   * The lock is ephemeral node and it should not be stored under task node
   *
   * <pre>Example: /{cluster}/connectors/{connectorType}/lock/{task-prefix}</pre>
   * @param cluster Brooklin cluster name
   * @param connectorType Connector
   * @param datastreamTaskPrefix Datastream task prefix name
   * @return datastream task lock prefix
   */
  public static String datastreamTaskLockPrefix(String cluster, String connectorType, String datastreamTaskPrefix) {
    return String.format(DATASTREAM_TASK_LOCK_PREFIX, cluster, connectorType, datastreamTaskPrefix).replaceAll("//", "/");
  }

  /**
   * Get the ZooKeeper znode for a specific datastream task's lock
   * The lock is ephemeral node and it should not be stored under task node
   *
   * <pre>Example: /{cluster}/connectors/{connectorType}/lock/{task-prefix}/{taskName}</pre>
   * @param cluster Brooklin cluster name
   * @param connectorType Connector
   * @param datastreamTaskPrefix Datastream task prefix name
   * @param datastreamTask Datastream task name
   * @return datastream task lock node
   */
  public static String datastreamTaskLock(String cluster, String connectorType, String datastreamTaskPrefix, String datastreamTask) {
    return String.format(DATASTREAM_TASK_LOCK, cluster, connectorType, datastreamTaskPrefix, datastreamTask).replaceAll("//", "/");
  }


  /**
   * Get the partition movement information for a specific datastream group
   * @param cluster Brooklin cluster name
   * @param connectorType Connector
   * @param datastreamGroupName Datastream group name
   * @return target assignment path
   */
  public static String getTargetAssignmentPath(String cluster, String connectorType, String datastreamGroupName) {
    return String.format(TARGET_ASSIGNMENTS, cluster, connectorType, datastreamGroupName).replaceAll("//", "/'");
  }

  /**
   * Get all partition movement information
   * @param cluster Brooklin cluster name
   * @param connectorType Connector
   * @return target assignment base
   */
  public static String getTargetAssignmentBase(String cluster, String connectorType) {
    return String.format(TARGET_ASSIGNMENT_BASE, cluster, connectorType).replaceAll("//", "/'");
  }
}
