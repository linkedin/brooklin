package com.linkedin.datastream.server;

import java.util.List;
import java.util.Objects;
import org.apache.commons.lang.Validate;
import org.codehaus.jackson.annotate.JsonIgnore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.datastream.common.JsonUtils;
import com.linkedin.datastream.common.LogUtils;

/**
 * This is a json serializable datastream task which can be stored into zookeeper
 *
 *  <p>InternalDatastreamTask has a unique name from getDatastreamTaskName. This is used as the znode name in zookeeper
 *  This should be unique for each instance of DatastreamTask, especially in the case when a Datastream is
 *  split into multiple DatastreamTasks. This is because we will have a state associated with each DatastreamTask.
 *
 */
public class InternalDatastreamTask {

  private static final Logger LOG = LoggerFactory.getLogger(InternalDatastreamTask.class.getName());

  // connector type. Type of the connector to be used for reading the change capture events
  // from the source, e.g. Oracle-Change, Espresso-Change, Oracle-Bootstrap, Espresso-Bootstrap,
  // Mysql-Change etc..
  private String _connectorType;

  // The Id of the datastream task. It is a string that will represent one assignable element of
  // datastream. By default, the value is empty string, representing that the DatastreamTask is by default
  // mapped to one Datastream. Each of the _id value will be represented in zookeeper
  // under /{cluster}/connectors/{connectorType}/{datastream}/{id}.
  private String _id = "";

  private String _taskPrefix;

  // List of partitions the task covers.
  private List<Integer> _partitions;

  // constructor for json serialization
  public InternalDatastreamTask() {

  }

  public InternalDatastreamTask(String id, String connectorType, String taskPrefix, List<Integer> partitions) {
    _id = id;
    _connectorType = connectorType;
    _taskPrefix = taskPrefix;
    _partitions = partitions;
  }

  /**
   * Construct InternalDatastreamTask from json string
   * @param  json JSON string of the task
   */
  public static InternalDatastreamTask fromJson(String json) {
    InternalDatastreamTask task = JsonUtils.fromJson(json, InternalDatastreamTask.class);
    LOG.debug("Loaded existing DatastreamTask: {}", task);
    return task;
  }

  /**
   * @return InternalDatastreamTask serialized as JSON
   */
  public String toJson() {
    return JsonUtils.toJson(this);
  }

  @JsonIgnore
  public String getDatastreamTaskName() {
    return _id.equals("") ? _taskPrefix : _taskPrefix + "_" + _id;
  }

  public void setPartitions(List<Integer> partitions) {
    Validate.notNull(partitions);
    _partitions = partitions;
  }

  public List<Integer> getPartitions() {
    return _partitions;
  }

  public String getConnectorType() {
    return _connectorType;
  }

  public void setConnectorType(String connectorType) {
    _connectorType = connectorType;
  }

  public void setId(String id) {
    _id = id;
  }

  public String getId() {
    return _id;
  }

  public String getTaskPrefix() {
    return _taskPrefix;
  }

  public void setTaskPrefix(String taskPrefix) {
    _taskPrefix = taskPrefix;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    InternalDatastreamTask task = (InternalDatastreamTask) o;
    return Objects.equals(_connectorType, task._connectorType) && Objects.equals(_id, task._id) && Objects.equals(
        _taskPrefix, task._taskPrefix) && Objects.equals(_partitions, task._partitions);
  }

  @Override
  public int hashCode() {
    return Objects.hash(_connectorType, _id, _taskPrefix, _partitions);
  }

  @Override
  public String toString() {
    // toString() is mainly for logging purpose, feel free to modify the content/format
    return String.format("%s(%s), partitions=%s", getDatastreamTaskName(), _connectorType,
        LogUtils.logNumberArrayInRange(_partitions));
  }
}
