/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.server.zk;

/**
 * Helper class to build commonly accessed Zookeeper node paths
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
   */

  /**
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
   * Task lock node under connectorType/task
   */
  private static final String DATASTREAM_TASK_LOCK = CONNECTOR + "/%s/lock";

  /**
   * Get the root level Zookeeper node for the specified brooklin cluster name
   * @param clusterName Brooklin cluster name of interest
   */
  public static String cluster(String clusterName) {
    return String.format(CLUSTER, clusterName);
  }

  /**
   * Get the Zookeeper node containing the list of live instances participating in the brooklin cluster.
   * The /liveinstances znode  is where the different Brooklin instances create ephemeral znodes with incremental
   * sequence numbers for the purposes of leader Coordinator election.
   * @param cluster Brooklin cluster name for which the "liveinstances" node is required
   * @see #liveInstance(String, String)
   * @see #instances(String)
   */
  public static String liveInstances(String cluster) {
    return String.format(LIVE_INSTANCES, cluster);
  }

  /**
   * Get the Zookeeper node for a specific live instance participating in the brooklin cluster
   * @param cluster Brooklin cluster in which the instance is a participant
   * @param instance Instance of interest
   * @see #liveInstances(String)
   */
  public static String liveInstance(String cluster, String instance) {
    return String.format(LIVE_INSTANCE, cluster, instance);
  }

  /**
   * Get the Zookeeper node containing the list of instances participating in the brooklin cluster.
   * The <i>/{cluster}/instances</i> znode is where every Brooklin instance creates a persistent znode for itself.
   * Each znode has a name composed of the concatenation of the instance hostname and its unique sequence number under
   * <i>/{cluster}/liveinstances</i>.
   * @param cluster Brooklin cluster name for which the "instances" znode is required
   * @see #instance(String, String)
   * @see #liveInstances(String)
   */
  public static String instances(String cluster) {
    return String.format(INSTANCES, cluster);
  }

  /**
   * Get the Zookeeper node for a specific instance participating in the brooklin cluster
   * @param cluster Brooklin cluster in which the instance is a participant
   * @param instanceName Instance of interest
   */
  public static String instance(String cluster, String instanceName) {
    return String.format(INSTANCE, cluster, instanceName);
  }

  /**
   * Get the Zookeeper node containing the list of datastram task assignments for a specific instance
   * @param cluster Brooklin cluster in which the instance is a participant
   * @param instance Instance for which the assignment is requested
   */
  public static String instanceAssignments(String cluster, String instance) {
    return String.format(INSTANCE_ASSIGNMENTS, cluster, instance);
  }

  /**
   * Get the Zookeeper node that persists messages about errors encountered by this Brooklin instance.
   * @param cluster Brooklin cluster in which the instance is a participant
   * @param instance Instance for which the error znode is requested
   */
  public static String instanceErrors(String cluster, String instance) {
    return String.format(INSTANCE_ERRORS, cluster, instance);
  }

  /**
   * Get the Zookeeper node for a specific datastream task assigned to the instance. This znode holds the JSON
   * serialization of the task itself.
   * @param cluster Brooklin cluster in which the instance is a participant
   * @param instance Instance name which has the assigned task of interest
   * @param datastreamTask Name of the datastream task of interest
   * @see #connectorTask(String, String, String)
   */
  public static String instanceAssignment(String cluster, String instance, String datastreamTask) {
    return String.format(INSTANCE_ASSIGNMENT, cluster, instance, datastreamTask);
  }

  /**
   * Get the Zookeeper node containing the list of all datastreams for a given cluster
   * @param cluster Brooklin cluster of interest
   */
  public static String datastreams(String cluster) {
    return String.format(DATASTREAMS, cluster);
  }

  /**
   * Get the Zookeeper node for a specific datastream in a cluster
   * @param cluster Brooklin cluster where the datastream is defined
   * @param stream Datastream of interest
   */
  public static String datastream(String cluster, String stream) {
    return String.format(DATASTREAM, cluster, stream);
  }

  /**
   * Get the Zookeeper node for a specific connector enabled in the Brooklin cluster
   * @param cluster Brooklin cluster containing the connector instance
   * @param connectorType Connector of interest
   */
  public static String connector(String cluster, String connectorType) {
    return String.format(CONNECTOR, cluster, connectorType);
  }

  /**
   * Get the Zookeeper node containing the list of all enabled connectors for a given Brooklin cluster
   * @param cluster Brooklin cluster of interest
   */
  public static String connectors(String cluster) {
    return String.format(CONNECTORS, cluster);
  }

  /**
   * Get the Zookeeper node for a specific datastream Task under the connector znode. This node holds the config and
   * state information of the task as opposed to the task node under the /instances znode which has the JSON
   * serialization of the task itself.
   * @param cluster
   * @param connectorType
   * @param datastreamTask
   * @see #instanceAssignment(String, String, String)
   */
  public static String connectorTask(String cluster, String connectorType, String datastreamTask) {
    return String.format(CONNECTOR_DATASTREAM_TASK, cluster, connectorType, datastreamTask);
  }

  /**
   * Get the Zookeeper node for the state of a specific datastream task under the connector znode. The state stores the
   * current state including progress information in the form of offsets/checkpoints or error messages in case of errors
   * @param cluster Brooklin cluster containing the datastream task of interest
   * @param connectorType Connector that has the datatream task assigned
   * @param datastreamTask Name of the datastream task of interest
   */
  public static String datastreamTaskState(String cluster, String connectorType, String datastreamTask) {
    // taskId could be empty space, which can result in "//" in the path
    return String.format(DATASTREAM_TASK_STATE, cluster, connectorType, datastreamTask).replaceAll("//", "/");
  }

  /**
   * Get the Zookeeper node for the configurations of a specific datastream task under the connector znode.
   * @param cluster Brooklin cluster containing the datastream task of interest
   * @param connectorType Connector that has the datatream task assigned
   * @param datastreamTask Name of the datastream task of interest
   */
  public static String datastreamTaskConfig(String cluster, String connectorType, String datastreamTask) {
    return String.format(DATASTREAM_TASK_CONFIG, cluster, connectorType, datastreamTask).replaceAll("//", "/");
  }

  /**
   * Get the Zookeeper node for a specific type of state information of a datastream task under the connector znode
   * An example of a specific type of state is the sourceCheckpoint. Different connectors can persist various state
   * categories of information they deem valuable under different keys.
   * @param cluster Brooklin cluster containing the datastream task of interest
   * @param connectorType Connector that has the datatream task assigned
   * @param datastreamTask Name of the datastream task of interest
   * @param key Key used to store the specific category of state information
   */
  public static String datastreamTaskStateKey(String cluster, String connectorType, String datastreamTask, String key) {
    // taskId could be empty space, which can result in "//" in the path
    return String.format(DATASTREAM_TASK_STATE_KEY, cluster, connectorType, datastreamTask, key).replaceAll("//", "/");
  }

  /**
   * Get the Zookeeper node for a specific datastream task's lock.
   * @param cluster Brooklin cluster containing the datastream task of interest
   * @param connectorType Connector that has the datatream task assigned
   * @param datastreamTask Name of the datastream task of interest
   * @return
   */
  public static String datastreamTaskLock(String cluster, String connectorType, String datastreamTask) {
    return String.format(DATASTREAM_TASK_LOCK, cluster, connectorType, datastreamTask).replaceAll("//", "/");
  }
}
