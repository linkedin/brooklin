package com.linkedin.datastream.server.zk;

public class KeyBuilder {
    private static final String _cluster ="/%s";
    private static final String _liveInstances = "/%s/liveinstances";
    private static final String _liveInstance = "/%s/liveinstances/%s";
    private static final String _instances = "/%s/instances";
    private static final String _instance = "/%s/instances/%s";
    private static final String _instanceTask = "/%s/instances/%s/%s";
    private static final String _datastreams = "/%s/datastream";
    private static final String _datastream = "/%s/datastream/%s";
    private static final String _datastreamTask = "/%s/instances/%s/%s";
    private static final String _connector = "/%s/%s";
    private static final String _connectorDatastreamTask = "/%s/%s/%s";

    public static String cluster(String clusterName) {
        return String.format(_cluster, clusterName);
    }
    public static String liveInstances(String cluster) { return String.format(_liveInstances, cluster); }
    public static String liveInstance(String cluster, String instance) { return String.format(_liveInstance, cluster, instance); }

    public static String instances(String cluster) {
        return String.format(_instances, cluster);
    }
    public static String instance(String cluster, String instanceName) {
        return String.format(_instance, cluster, instanceName);
    }

    public static String instanceTask(String cluster, String instance, String task) {
        return String.format(_instanceTask, cluster, instance, task);
    }

    public static String datastreams(String cluster) {
        return String.format(_datastreams, cluster);
    }

    public static String datastream(String cluster, String stream) {
        return String.format(_datastream, cluster, stream);
    }

    public static String datastreamTask(String cluster, String instance, String name) {
        return String.format(_datastreamTask, cluster, instance, name);
    }

    public static String connector(String cluster, String connectorType) {
        return String.format(_connector, cluster, connectorType);
    }

    public static String connectorTask(String cluster, String connectorType, String task) {
        return String.format(_connectorDatastreamTask, cluster, connectorType, task);
    }

}