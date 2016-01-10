package com.linkedin.datastream.server;

import com.linkedin.datastream.common.Datastream;
import com.linkedin.datastream.common.DatastreamDestination;
import com.linkedin.datastream.common.DatastreamException;
import com.linkedin.datastream.common.DatastreamSource;
import com.linkedin.datastream.common.JsonUtils;
import com.linkedin.datastream.server.zk.ZkAdapter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.codehaus.jackson.annotate.JsonIgnore;
import org.apache.commons.lang.Validate;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.Objects;
import java.util.UUID;


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

  // Wait at most 60 seconds for acquiring the task
  private static final Integer ACQUIRE_TIMEOUT_MS = 60000;

  // connector type. Type of the connector to be used for reading the change capture events
  // from the source, e.g. Oracle-Change, Espresso-Change, Oracle-Bootstrap, Espresso-Bootstrap,
  // Mysql-Change etc..
  private String _connectorType;

  // The Id of the datastreamtask. It is a string that will represent one assignable element of
  // datastream. By default, the value is empty string, representing that the DatastreamTask is by default
  // mapped to one Datastream. Each of the _id value will be represented in zookeeper
  // under /{cluster}/connectors/{connectorType}/{datastream}/{id}.
  private String _id = "";

  // _datastreamName is copied from Datastream instance. This is because in the znode we only persist
  // the datastream name, and only obtain instance of Datastream by reading from the znode
  // under corresponding /{cluster}/datastream/{datastreamName}
  private String _datastreamName;

  private Datastream _datastream;

  // List of partitions the task covers.
  private List<Integer> _partitions;

  private ZkAdapter _zkAdapter;

  private Map<String, String> _properties = new HashMap<>();
  private DatastreamEventProducer _eventProducer;

  public DatastreamTaskImpl() {
    _partitions = new ArrayList<>();
  }

  public DatastreamTaskImpl(Datastream datastream) {
    this(datastream, UUID.randomUUID().toString());
  }

  public DatastreamTaskImpl(Datastream datastream, String id) {
    this(datastream, id, null);
  }

  public DatastreamTaskImpl(Datastream datastream, String id, List<Integer> partitions) {
    Validate.isTrue(datastream != null, "null datastream");
    Validate.isTrue(id != null, "null id");
    _datastreamName = datastream.getName();
    _connectorType = datastream.getConnectorType();
    _datastream = datastream;
    _id = id;
    _partitions = new ArrayList<>();
    if (partitions != null && partitions.size() > 0) {
      _partitions.addAll(partitions);
    } else {
      // Add [0, N) if destination has N partitions
      // Or add a default partition 0 otherwise
      if (datastream.hasDestination() && datastream.getDestination().hasPartitions()) {
        int numPartitions = datastream.getDestination().getPartitions();
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
   * Construct DatastreamTask from json string
   * @param  json JSON string of the task
   */
  public static DatastreamTaskImpl fromJson(String json) {
    DatastreamTaskImpl task = JsonUtils.fromJson(json, DatastreamTaskImpl.class);
    LOG.debug("Loaded existing DatastreamTask: " + task);
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
  public Datastream getDatastream() {
    return _datastream;
  }

  @JsonIgnore
  public String getDatastreamTaskName() {
    return _id.equals("") ? _datastreamName : _datastreamName + "_" + _id;
  }

  @JsonIgnore
  @Override
  public DatastreamSource getDatastreamSource() {
    return _datastream.getSource();
  }

  @JsonIgnore
  @Override
  public DatastreamDestination getDatastreamDestination() {
    return _datastream.getDestination();
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
    // There is only one implementation of EventProducer so it's safe to cast
    DatastreamEventProducerImpl impl = (DatastreamEventProducerImpl) _eventProducer;
    Map<DatastreamTask, Map<Integer, String>> safeCheckpoints = impl.getEventProducer().getSafeCheckpoints();
    // Checkpoint map of the owning task must be present in the producer
    Validate.isTrue(safeCheckpoints.containsKey(this), "null checkpoints for task: " + this);
    return safeCheckpoints.get(this);
  }

  @JsonIgnore
  @Override
  public List<String> getDatastreams() {
    return Arrays.asList(_datastreamName);
  }

  @Override
  public void acquire() throws DatastreamException {
    acquire(ACQUIRE_TIMEOUT_MS);
  }

  public void acquire(int timeout) throws DatastreamException {
    Validate.notNull(_zkAdapter, "Task is not properly initialized for processing.");
    try {
      _zkAdapter.acquireTask(this, timeout);
    } catch (DatastreamException e) {
      LOG.error("Failed to acquire task: " + this, e);
      setStatus(new DatastreamTaskStatus(DatastreamTaskStatus.Code.ERROR, "Acquire failed, exception: " + e));
      throw e;
    }
  }


  @Override
  public void release() {
    Validate.notNull(_zkAdapter, "Task is not properly initialized for processing.");
    _zkAdapter.releaseTask(this);
  }

  public void setDatastream(Datastream datastream) {
    _datastream = datastream;
  }

  @JsonIgnore
  public DatastreamEventProducer getEventProducer() {
    return _eventProducer;
  }

  public void setEventProducer(DatastreamEventProducer eventProducer) {
    _eventProducer = eventProducer;
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

  public String getDatastreamName() {
    return _datastreamName;
  }

  public void setDatastreamName(String datastreamName) {
    this._datastreamName = datastreamName;
  }

  @JsonIgnore
  @Override
  public String getState(String key) {
    return _zkAdapter.getDatastreamTaskStateForKey(this, key);
  }

  @JsonIgnore
  @Override
  public void saveState(String key, String value) {
    Validate.notNull(key, "key cannot be null");
    Validate.notNull(value, "value cannot be null");
    Validate.notEmpty(key, "Key cannot be empty");
    Validate.notEmpty(value, "value cannot be empty");
    _zkAdapter.setDatastreamTaskStateForKey(this, key, value);
  }

  @JsonIgnore
  @Override
  public void setStatus(DatastreamTaskStatus status) {
    saveState(STATUS, status.toString());
  }

  @JsonIgnore
  @Override
  public DatastreamTaskStatus getStatus() {
    String statusStr = getState(STATUS);
    if (statusStr != null && !statusStr.isEmpty()) {
      return DatastreamTaskStatus.valueOf(statusStr);
    } else {
      throw new RuntimeException("Datastream task status is either null or empty");
    }
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
    return Objects.equals(_connectorType, task._connectorType) && Objects.equals(_id, task._id)
        && Objects.equals(_datastreamName, task._datastreamName) && Objects.equals(_datastream, task._datastream)
        && Objects.equals(_properties, task._properties) && Objects.equals(_partitions, task._partitions);
  }

  @Override
  public int hashCode() {
    return Objects.hash(_connectorType, _id, _datastreamName, _datastream, _properties, _partitions);
  }

  @Override
  public String toString() {
    // toString() is mainly for loggign purpose, feel free to modify the content/format
    return String.format("%s(%s), partitions=%s", getDatastreamTaskName(), _connectorType, _partitions);
  }

  public void setZkAdapter(ZkAdapter adapter) {
    _zkAdapter = adapter;
  }
}
