package com.linkedin.datastream.server.zk;

public class KeyBuilder {
  private static final String _cluster = "/%s";
  private static final String _liveInstances = "/%s/liveinstances";
  private static final String _liveInstance = "/%s/liveinstances/%s";
  private static final String _instances = "/%s/instances";
  private static final String _instance = "/%s/instances/%s";
  private static final String _instanceAssignments = "/%s/instances/%s/assignments";
  private static final String _instanceErrors = "/%s/instances/%s/errors";
  private static final String _instanceAssignment = "/%s/instances/%s/assignments/%s";
  private static final String _datastreams = "/%s/dms";
  private static final String _datastream = "/%s/dms/%s";
  private static final String _connectors = "/%s/connectors";
  private static final String _connector = "/%s/connectors/%s";

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
  private static final String _connectorDatastreamTask = _connector + "/%s";

  /**
   * Task state node under connectorType/task
   */
  private static final String _datastreamTaskState = _connector + "/%s/state";

  /**
   * Specific task state node under connectorType/task/state
   */
  private static final String _datastreamTaskStateKey = _connector + "/%s/state/%s";

  /**
   * Task config node under connectorType/task
   */
  private static final String _datastreamTaskConfig = _connector + "/%s/config";

  /**
   * Task lock node under connectorType/task
   */
  private static final String _datastreamTaskLock = _connector + "/%s/lock";

  public static String cluster(String clusterName) {
    return String.format(_cluster, clusterName);
  }

  public static String liveInstances(String cluster) {
    return String.format(_liveInstances, cluster);
  }

  public static String liveInstance(String cluster, String instance) {
    return String.format(_liveInstance, cluster, instance);
  }

  public static String instances(String cluster) {
    return String.format(_instances, cluster);
  }

  public static String instance(String cluster, String instanceName) {
    return String.format(_instance, cluster, instanceName);
  }

  public static String instanceAssignments(String cluster, String instance) {
    return String.format(_instanceAssignments, cluster, instance);
  }

  public static String instanceErrors(String cluster, String instance) {
    return String.format(_instanceErrors, cluster, instance);
  }

  public static String instanceAssignment(String cluster, String instance, String name) {
    return String.format(_instanceAssignment, cluster, instance, name);
  }

  public static String datastreams(String cluster) {
    return String.format(_datastreams, cluster);
  }

  public static String datastream(String cluster, String stream) {
    return String.format(_datastream, cluster, stream);
  }

  public static String connector(String cluster, String connectorType) {
    return String.format(_connector, cluster, connectorType);
  }

  public static String connectors(String cluster) {
    return String.format(_connectors, cluster);
  }

  public static String connectorTask(String cluster, String connectorType, String name) {
    return String.format(_connectorDatastreamTask, cluster, connectorType, name);
  }

  // zookeeper path: /{cluster}/connectors/{connectorType}/{taskName}/state
  public static String datastreamTaskState(String cluster, String connectorType, String name) {
    // taskId could be empty space, which can result in "//" in the path
    return String.format(_datastreamTaskState, cluster, connectorType, name).replaceAll("//", "/");
  }

  // zookeeper path: /{cluster}/connectors/{connectorType}/{taskName}/config
  public static String datastreamTaskConfig(String cluster, String connectorType, String name) {
    return String.format(_datastreamTaskConfig, cluster, connectorType, name).replaceAll("//", "/");
  }

  // zookeeper path: /{cluster}/connectors/{connectorType}/{taskName}/state
  public static String datastreamTaskStateKey(String cluster, String connectorType, String name, String key) {
    // taskId could be empty space, which can result in "//" in the path
    return String.format(_datastreamTaskStateKey, cluster, connectorType, name, key).replaceAll("//", "/");
  }

  // zookeeper path: /{cluster}/connectors/{connectorType}/{taskName}/config
  public static String datastreamTaskLock(String cluster, String connectorType, String name) {
    return String.format(_datastreamTaskLock, cluster, connectorType, name).replaceAll("//", "/");
  }
}
