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
  private static final String _connectorDatastreamTask = _connector +  "/%s";
  private static final String _datastreamTaskState = _connector + "/tasks/%s/state";
  private static final String _datastreamTaskStateKey = _connector + "/tasks/%s/state/%s";
  private static final String _datastreamTaskConfig = _connector + "/tasks/%s/config";

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

  public static String connectorTask(String cluster, String connectorType, String task) {
    return String.format(_connectorDatastreamTask, cluster, connectorType, task);
  }

  // zookeeper path: /{cluster}/connectors/{connectorType}/{taskId}/state
  public static String datastreamTaskState(String cluster, String connectorType, String taskId) {
    // taskId could be empty space, which can result in "//" in the path
    return String.format(_datastreamTaskState, cluster, connectorType, taskId).replaceAll("//", "/");
  }

  // zookeeper path: /{cluster}/connectors/{connectorType}/{taskId}/config
  public static String datastreamTaskConfig(String cluster, String connectorType, String taskId) {
    return String.format(_datastreamTaskConfig, cluster, connectorType, taskId).replaceAll("//", "/");
  }

  // zookeeper path: /{cluster}/connectors/{connectorType}/{taskId}/state
  public static String datastreamTaskStateKey(String cluster, String connectorType,
      String taskId, String key) {
    // taskId could be empty space, which can result in "//" in the path
    return String.format(_datastreamTaskStateKey, cluster, connectorType, taskId, key).replaceAll("//",
        "/");
  }

}
