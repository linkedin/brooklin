/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.server;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

import org.apache.commons.lang.Validate;
import org.codehaus.jackson.annotate.JsonIgnore;
import org.jetbrains.annotations.TestOnly;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.datastream.common.Datastream;
import com.linkedin.datastream.common.DatastreamDestination;
import com.linkedin.datastream.common.DatastreamMetadataConstants;
import com.linkedin.datastream.common.DatastreamSource;
import com.linkedin.datastream.common.DatastreamUtils;
import com.linkedin.datastream.common.JsonUtils;
import com.linkedin.datastream.common.LogUtils;
import com.linkedin.datastream.serde.SerDeSet;
import com.linkedin.datastream.server.zk.ZkAdapter;


/**
 * DatastreamTask is the minimum assignable element of a Datastream. It is mainly used to partition the datastream
 * defined by Datastream. For example, the user can define an instance of Datastream for an Oracle bootstrap
 * connector, but this logical datastream can be splitted to a number of DatastreamTask instances, each is tied
 * to one partition. This way, each instance of DatastreamTask can be assigned independently, which in turn can
 * result in bigger output and better concurrent IO.
 *
 * <p>DatastreamTask are used as input for specific connectors. Besides the reference of the original
 * Datastream object, DatastreamTask also contains a key-value store Properties. This allows the assignment
 * strategy to attach extra parameters.
 *
 * <p>DatastreamTask has a unique name called _datastreamtaskName. This is used as the znode name in zookeeper
 * This should be unique for each instance of DatastreamTask, especially in the case when a Datastream is
 * split into multiple DatastreamTasks. This is because we will have a state associated with each DatastreamTask.
 *
 */
public class DatastreamTaskImpl implements DatastreamTask {

  private static final Logger LOG = LoggerFactory.getLogger(DatastreamTask.class.getName());

  private static final String STATUS = "STATUS";
  private volatile List<Datastream> _datastreams;

  private HashMap<Integer, String> _checkpoints = new HashMap<>();

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

  private ZkAdapter _zkAdapter;

  private Map<String, String> _properties = new HashMap<>();
  private DatastreamEventProducer _eventProducer;
  private String _transportProviderName;
  private SerDeSet _destinationSerDes = new SerDeSet(null, null, null);

  @TestOnly
  public DatastreamTaskImpl() {
    _partitions = new ArrayList<>();
  }

  public DatastreamTaskImpl(List<Datastream> datastreams) {
    this(datastreams, UUID.randomUUID().toString(), new ArrayList<>());
  }

  public DatastreamTaskImpl(List<Datastream> datastreams, String id, List<Integer> partitions) {
    Validate.notEmpty(datastreams, "empty datastream");
    Validate.notNull(id, "null id");

    Datastream datastream = datastreams.get(0);
    _connectorType = datastream.getConnectorName();
    _transportProviderName = datastream.getTransportProviderName();
    _datastreams = datastreams;
    _taskPrefix = datastream.getMetadata().get(DatastreamMetadataConstants.TASK_PREFIX);
    _id = id;
    _partitions = new ArrayList<>();
    if (partitions != null && partitions.size() > 0) {
      _partitions.addAll(partitions);
    } else {
      // Add [0, N) if source has N partitions
      // Or add a default partition 0 otherwise
      if (datastream.hasSource() && datastream.getSource().hasPartitions()) {
        int numPartitions = datastream.getSource().getPartitions();
        for (int i = 0; i < numPartitions; i++) {
          _partitions.add(i);
        }
      } else {
        _partitions.add(0);
      }
    }
    LOG.info("Created new DatastreamTask " + this);
  }

  /**
   * @return the prefix of the task names that will be created for this datastream.
   */
  public static String getTaskPrefix(Datastream datastream) {
    return datastream.getName();
  }

  /**
   * Construct DatastreamTask from json string
   * @param  json JSON string of the task
   */
  public static DatastreamTaskImpl fromJson(String json) {
    DatastreamTaskImpl task = JsonUtils.fromJson(json, DatastreamTaskImpl.class);
    LOG.info("Loaded existing DatastreamTask: {}", task);
    return task;
  }

  /**
   * @return DatastreamTask serialized as JSON
   * @throws IOException
   */
  public String toJson() throws IOException {
    return JsonUtils.toJson(this);
  }

  @JsonIgnore
  public String getDatastreamTaskName() {
    return _id.equals("") ? _taskPrefix : _taskPrefix + "_" + _id;
  }

  @JsonIgnore
  @Override
  public boolean isUserManagedDestination() {
    return DatastreamUtils.isUserManagedDestination(_datastreams.get(0));
  }

  @JsonIgnore
  @Override
  public DatastreamSource getDatastreamSource() {
    return _datastreams.get(0).getSource();
  }

  @JsonIgnore
  @Override
  public DatastreamDestination getDatastreamDestination() {
    return _datastreams.get(0).getDestination();
  }

  public void setPartitions(List<Integer> partitions) {
    Validate.notNull(partitions);
    _partitions = partitions;
  }

  @Override
  public List<Integer> getPartitions() {
    return _partitions;
  }

  @JsonIgnore
  @Override
  public Map<Integer, String> getCheckpoints() {
    return _checkpoints;
  }

  /**
   * Get the list of datastreams for the datastream task. Note that the datastreams may change
   * between onAssignmentChange (because of datastream update for example). It's connector's
   * responsibility to re-fetch the datastream list even when it receives the exact same set
   * of datastream tasks.
   *
   * TODO: Datastream might be null if derived from zk or json, we need a better abstraction here
   */
  @JsonIgnore
  @Override
  public List<Datastream> getDatastreams() {
    if (_datastreams == null || _datastreams.size() == 0) {
      throw new IllegalArgumentException("Fetch datastream from zk stored task is not allowed");
    }
    return Collections.unmodifiableList(_datastreams);
  }

  @Override
  public void acquire(Duration timeout) {
    Validate.notNull(_zkAdapter, "Task is not properly initialized for processing.");
    try {
      _zkAdapter.acquireTask(this, timeout);
    } catch (Exception e) {
      LOG.error("Failed to acquire task: " + this, e);
      setStatus(DatastreamTaskStatus.error("Acquire failed, exception: " + e));
      throw e;
    }
  }

  @Override
  public void release() {
    Validate.notNull(_zkAdapter, "Task is not properly initialized for processing.");
    _zkAdapter.releaseTask(this);
  }

  public void setDatastreams(List<Datastream> datastreams) {
    _datastreams = datastreams;
    // destination and connector type should be immutable
    _transportProviderName = _datastreams.get(0).getTransportProviderName();
    _connectorType = _datastreams.get(0).getConnectorName();
  }

  @JsonIgnore
  public DatastreamEventProducer getEventProducer() {
    return _eventProducer;
  }

  public void setEventProducer(DatastreamEventProducer eventProducer) {
    _eventProducer = eventProducer;
  }

  public void assignSerDes(SerDeSet destination) {
    _destinationSerDes = destination;
  }

  public String getConnectorType() {
    return _connectorType;
  }

  @Override
  public String getTransportProviderName() {
    return _transportProviderName;
  }

  public void setTransportProviderName(String transportProviderName) {
    _transportProviderName = transportProviderName;
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

  @JsonIgnore
  @Override
  public String getState(String key) {
    return _zkAdapter.getDatastreamTaskStateForKey(this, key);
  }

  @JsonIgnore
  @Override
  public void saveState(String key, String value) {
    Validate.notEmpty(key, "Key cannot be null or empty");
    Validate.notEmpty(value, "value cannot be null or empty");
    _zkAdapter.setDatastreamTaskStateForKey(this, key, value);
  }

  @JsonIgnore
  @Override
  public SerDeSet getDestinationSerDes() {
    return _destinationSerDes;
  }

  @JsonIgnore
  @Override
  public void setStatus(DatastreamTaskStatus status) {
    saveState(STATUS, JsonUtils.toJson(status));
  }

  @JsonIgnore
  @Override
  public DatastreamTaskStatus getStatus() {
    String statusStr = getState(STATUS);
    if (statusStr != null && !statusStr.isEmpty()) {
      return JsonUtils.fromJson(statusStr, DatastreamTaskStatus.class);
    }
    return null;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    DatastreamTaskImpl task = (DatastreamTaskImpl) o;
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

  public void setZkAdapter(ZkAdapter adapter) {
    _zkAdapter = adapter;
  }

  public void updateCheckpoint(int partition, String checkpoint) {
    LOG.debug("Update checkpoint called for partition {} and checkpoint {}", partition, checkpoint);
    _checkpoints.put(partition, checkpoint);
  }

  public void setCheckpoints(Map<Integer, String> checkpoints) {
    _checkpoints = new HashMap<>(checkpoints);
  }
}
