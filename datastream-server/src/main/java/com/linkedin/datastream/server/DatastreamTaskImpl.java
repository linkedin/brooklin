package com.linkedin.datastream.server;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.commons.lang.Validate;
import org.jetbrains.annotations.TestOnly;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.datastream.common.Datastream;
import com.linkedin.datastream.common.DatastreamDestination;
import com.linkedin.datastream.common.DatastreamMetadataConstants;
import com.linkedin.datastream.common.DatastreamSource;
import com.linkedin.datastream.common.DatastreamUtils;
import com.linkedin.datastream.common.JsonUtils;
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
 * <p>DatatsteamTask consists of two parts, one is InternalDatastreamTask, which is stored into zookeeper, and
 * the other stats variable which are volatile and may be gone during leader change</p>
 *
 *
 */
public class DatastreamTaskImpl implements DatastreamTask {

  private static final Logger LOG = LoggerFactory.getLogger(DatastreamTask.class.getName());

  private static final String STATUS = "STATUS";
  private volatile List<Datastream> _datastreams;

  private final InternalDatastreamTask _internalDatastreamTask;

  private HashMap<Integer, String> _checkpoints = new HashMap<>();
  private ZkAdapter _zkAdapter;
  private DatastreamEventProducer _eventProducer;
  private String _transportProviderName;
  private SerDeSet _destinationSerDes = new SerDeSet(null, null, null);

  @TestOnly
  public DatastreamTaskImpl() {
    _internalDatastreamTask = new InternalDatastreamTask();
  }

  public DatastreamTaskImpl(List<Datastream> datastreams) {
    this(datastreams, UUID.randomUUID().toString(), new ArrayList<>());
  }

  public DatastreamTaskImpl(List<Datastream> datastreams, String id, List<Integer> partitions) {
    Validate.notEmpty(datastreams, "empty datastream");
    Validate.notNull(id, "null id");

    Datastream datastream = datastreams.get(0);
    String taskPrefix = datastream.getMetadata().get(DatastreamMetadataConstants.TASK_PREFIX);
    List<Integer> defaultPartitions = new ArrayList<>();
    if (partitions != null && partitions.size() > 0) {
      defaultPartitions.addAll(partitions);
    } else {
      // Add [0, N) if source has N partitions
      // Or add a default partition 0 otherwise
      if (datastream.hasSource() && datastream.getSource().hasPartitions()) {
        int numPartitions = datastream.getSource().getPartitions();
        for (int i = 0; i < numPartitions; i++) {
          defaultPartitions.add(i);
        }
      } else {
        defaultPartitions.add(0);
      }
    }
    _transportProviderName = datastream.getTransportProviderName();
    _internalDatastreamTask = new InternalDatastreamTask(id, datastream.getConnectorName(), taskPrefix, defaultPartitions);
    LOG.info("Created new DatastreamTask ", _internalDatastreamTask.toString());
    _datastreams = datastreams;
  }

  public DatastreamTaskImpl(InternalDatastreamTask internalDatastreamTask, List<Datastream> datastreams) {
    Validate.notEmpty(datastreams, "empty datastream");
    Datastream datastream = datastreams.get(0);
    String taskPrefix = datastream.getMetadata().get(DatastreamMetadataConstants.TASK_PREFIX);
    if (!taskPrefix.equals(internalDatastreamTask.getTaskPrefix())) {
      LOG.warn("datastream prefix doesn't match to the task got from zk");
    }
    _internalDatastreamTask = internalDatastreamTask;
    _datastreams = datastreams;
    _transportProviderName = _datastreams.get(0).getTransportProviderName();
    _internalDatastreamTask.setConnectorType(_datastreams.get(0).getConnectorName());
  }

  public DatastreamTaskImpl(InternalDatastreamTask internalDatastreamTask, List<Datastream> datastreams, ZkAdapter zkAdapter) {
    this(internalDatastreamTask, datastreams);
    _zkAdapter = zkAdapter;
  }
  /**
   * @return the prefix of the task names that will be created for this datastream.
   */
  public static String getTaskPrefix(Datastream datastream) {
    return datastream.getName();
  }

  @Override
  public boolean isUserManagedDestination() {
    return DatastreamUtils.isUserManagedDestination(_datastreams.get(0));
  }

  @Override
  public DatastreamSource getDatastreamSource() {
    return _datastreams.get(0).getSource();
  }

  @Override
  public DatastreamDestination getDatastreamDestination() {
    return _datastreams.get(0).getDestination();
  }

  @Override
  public Map<Integer, String> getCheckpoints() {
    return _checkpoints;
  }

  /**
   * Get the list of datastreams for the datastream task. Note that the datastreams may change
   * between onAssignmentChange (because of datastream update for example). It's connector's
   * responsibility to re-fetch the datastream list even when it receives the exact same set
   * of datastream tasks.
   */
  @Override
  public List<Datastream> getDatastreams() {
    return Collections.unmodifiableList(_datastreams);
  }

  @Override
  public void acquire(Duration timeout) {
    Validate.notNull(_zkAdapter, "Task is not properly initialized for processing.");
    try {
      _zkAdapter.acquireTask(this.getInternalTask(), timeout);
    } catch (Exception e) {
      LOG.error("Failed to acquire task: " + this, e);
      setStatus(DatastreamTaskStatus.error("Acquire failed, exception: " + e));
      throw e;
    }
  }

  @Override
  public void release() {
    Validate.notNull(_zkAdapter, "Task is not properly initialized for processing.");
    _zkAdapter.releaseTask(this.getInternalTask());
  }

  public void setDatastreams(List<Datastream> datastreams) {
    _datastreams = datastreams;
    // destination and connector type should be immutable
    _transportProviderName = _datastreams.get(0).getTransportProviderName();
    _internalDatastreamTask.setConnectorType(_datastreams.get(0).getConnectorName());
  }

  public DatastreamEventProducer getEventProducer() {
    return _eventProducer;
  }

  public void setEventProducer(DatastreamEventProducer eventProducer) {
    _eventProducer = eventProducer;
  }

  public void assignSerDes(SerDeSet destination) {
    _destinationSerDes = destination;
  }

  @Override
  public String getConnectorType() {
    return _internalDatastreamTask.getConnectorType();
  }

  @Override
  public String getId() {
    return _internalDatastreamTask.getId();
  }

  @Override
  public String getDatastreamTaskName() {
    return _internalDatastreamTask.getDatastreamTaskName();
  }

  @Override
  public String getTransportProviderName() {
    return _transportProviderName;
  }

  @Override
  public List<Integer> getPartitions() {
    return _internalDatastreamTask.getPartitions();
  }

  @Override
  public String getTaskPrefix() {
    return _internalDatastreamTask.getTaskPrefix();
  }

  public InternalDatastreamTask getInternalTask() {
    return _internalDatastreamTask;
  }

  @Override
  public String getState(String key) {
    return _zkAdapter.getDatastreamTaskStateForKey(_internalDatastreamTask, key);
  }

  @Override
  public void saveState(String key, String value) {
    Validate.notEmpty(key, "Key cannot be null or empty");
    Validate.notEmpty(value, "value cannot be null or empty");
    _zkAdapter.setDatastreamTaskStateForKey(_internalDatastreamTask, key, value);
  }

  @Override
  public SerDeSet getDestinationSerDes() {
    return _destinationSerDes;
  }

  @Override
  public void setStatus(DatastreamTaskStatus status) {
    saveState(STATUS, JsonUtils.toJson(status));
  }

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
    return this.getInternalTask().equals(task.getInternalTask());
  }

  @Override
  public int hashCode() {
    return this.getInternalTask().hashCode();
  }

  @Override
  public String toString() {
    return this.getInternalTask().toString();
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
