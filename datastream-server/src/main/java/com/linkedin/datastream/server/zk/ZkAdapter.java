package com.linkedin.datastream.server.zk;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.IZkDataListener;
import org.I0Itec.zkclient.exception.ZkException;
import org.apache.zookeeper.CreateMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.datastream.common.Datastream;
import com.linkedin.datastream.common.DatastreamException;
import com.linkedin.datastream.common.DatastreamUtils;
import com.linkedin.datastream.common.ErrorLogger;
import com.linkedin.datastream.common.zk.ZkClient;
import com.linkedin.datastream.server.DatastreamTask;
import com.linkedin.datastream.server.DatastreamTaskImpl;


/**
 *
 *
 *
 *        ZooKeeper                                     ZkAdapter
 * ┌─────────────────────┐          ┌───────────────────────────────────────────────────┐
 * │- cluster            │          │                                                   │
 * │  |- instances ──────┼──┐       │                                                   │
 * │  |  |- i001         │  │       │┌───────────────────────────────────┐    ┌─────────┤
 * │  |  |- i002         │  └───────┼▶  ZkBackedInstanceAssignmentList   │    │         │   ┌────────────────┐
 * │  |                  │          │└───────────────────────────────────┘    │         │───▶ onBecomeLeader │
 * │  |- liveinstances ──┼───┐      │┌───────────────────────────────────┐    │         │   └────────────────┘
 * │  |  |- i001         │   └──────▶│ ZkBackedLiveInstanceListProvider  │    │         │   ┌──────────────────┐
 * │  |  |- i002─────────┼┐         │└───────────────────────────────────┘    │         │───▶ onBecomeFollower │
 * │  |                  ││         │┌───────────────────────────────────┐    │ZkAdapter│   └──────────────────┘
 * │  |- connectors      │└─────────▶│     ZkLeaderElectionListener      │    │Listener │   ┌────────────────────┐
 * │  |  |- Espresso ────┼───┐      │└───────────────────────────────────┘    │         │───▶ onAssignmentChange │
 * │  |  |- Oracle ──────┼──┐│      │┌───────────────────────────────────┐    │         │   └────────────────────┘
 * │                     │  └┴──────┼▶    ZkBackedDatastreamTasksMap     │    │         │   ┌───────────────────────┐
 * │                     │          │└───────────────────────────────────┘    │         │───▶ onLiveInstancesChange │
 * │                     │          │                                         │         │   └───────────────────────┘
 * │                     │          │                                         └─────────┤
 * │                     │          │                                                   │
 * │                     │          │                                                   │
 * │                     │          │                                                   │
 * └─────────────────────┘          │                                                   │
 *                                  └───────────────────────────────────────────────────┘
 *
 *
 *
 * ZkAdapter is the adapter between the Coordiantor and the ZkClient. It uses ZkClient to communicate
 * with Zookeeper, and provides a set of callbacks that allows the Coordinator to react on events like
 * leadership changes, assigment changes, and live instances changes.
 *
 * <p> ZkAdapter provide two main roles:
 * <ul>
 *     <li>ZooKeeper backed data provider. Each of these zk backed data provider is implemented as an embedded
 *     class. For example, {@link com.linkedin.datastream.server.zk.ZkAdapter.ZkBackedLiveInstanceListProvider}
 *     provides the current list of live instances, and its data is automatically updated when the underlying
 *     zookeeper data structure is updated.
 *     </li>
 *
 *     <li>Notify the observers. ZkAdapter will trigger the {@link com.linkedin.datastream.server.zk.ZkAdapter.ZkAdapterListener}
 *     callbacks based on the current state. The {@link com.linkedin.datastream.server.Coordinator} implements
 *     this interface so it can take appropriate actions.
 *     </li>
 * </ul>
 *
 * <p>The ZK backed data providers cache the data read from the corresponding zookeeper nodes so they can be accessed
 * without reading zookeeper frequently. These providers also set up the watch on these nodes so it can be notified
 * when the data changes. For example {@link com.linkedin.datastream.server.zk.ZkAdapter.ZkBackedTaskListProvider}
 * provide the list of DatastreamTask objects that are assigned to this instance. This provider also watches the
 * znode /{cluster}/instances/{instanceName} for children changes, and automatically refresh the cached values.
 *
 * @see com.linkedin.datastream.server.Coordinator
 * @see ZkClient
 */

public class ZkAdapter {
  private Logger _log = LoggerFactory.getLogger(ZkAdapter.class.getName());

  private String _zkServers;
  private String _cluster;
  private int _sessionTimeout;
  private int _connectionTimeout;
  private ZkClient _zkclient;

  private String _instanceName;
  private String _liveInstanceName;
  private String _hostname;

  private boolean _isLeader = false;
  private ZkAdapterListener _listener;

  // the current znode this node is listening to
  private String _currentSubscription = null;

  private Random randomGenerator = new Random();

  private ZkLeaderElectionListener _leaderElectionListener = new ZkLeaderElectionListener();
  private ZkBackedLiveInstanceListProvider _liveInstancesProvider = null;

  // only the leader should maintain this list
  private ZkBackedDMSDatastreamList _datastreamList = null;
  private ZkBackedTaskListProvider _assignmentList = null;

  // Cache all live DatastreamTasks per instance for assignment strategy
  private Map<String, Set<DatastreamTask>> _liveTaskMap = new HashMap<>();

  public ZkAdapter(String zkServers, String cluster) {
    this(zkServers, cluster, ZkClient.DEFAULT_SESSION_TIMEOUT, ZkClient.DEFAULT_CONNECTION_TIMEOUT, null);
  }

  public ZkAdapter(String zkServers, String cluster, int sessionTimeout, int connectionTimeout) {
    this(zkServers, cluster, sessionTimeout, connectionTimeout, null);
  }

  public ZkAdapter(String zkServers, String cluster, int sessionTimeout, int connectionTimeout,
      ZkAdapterListener listener) {
    _zkServers = zkServers;
    _cluster = cluster;
    _sessionTimeout = sessionTimeout;
    _connectionTimeout = connectionTimeout;
    _listener = listener;
  }

  /**
   * ZkAdapter adapts the zookeeper data changes with the Coordinator logic. This method
   * is responsible to hook up the listener so that the change will trigger the corresponding implementation.
   *
   * @param listener implementation of ZkAdapterListener. In most cases it is an instance of Coordinator
   */
  public void setListener(ZkAdapterListener listener) {
    _listener = listener;
  }

  public boolean isLeader() {
    return _isLeader;
  }

  public String getInstanceName() {
    return _instanceName;
  }

  /**
   * gracefully disconnect from zookeeper, clean up znodes
   */
  public void disconnect() {

    if (_zkclient != null) {
      try {
        // remove the liveinstance node
        String liveInstancePath = KeyBuilder.liveInstance(_cluster, _liveInstanceName);
        _log.info("deleting live instance node: " + liveInstancePath);
        _zkclient.delete(liveInstancePath);

        // NOTE: we should not delete the instance node which still holds the
        // assigned tasks. Coordinator will call cleanupDeadInstanceAssignments
        // to do an ad-hoc cleanup once the tasks haven been properly handled
        // per the strategy (reassign or discard).
      } catch (ZkException zke) {
        // do nothing, best effort clean up
      } finally {
        _zkclient.close();
        _zkclient = null;
      }
    }
    _isLeader = false;
  }

  /**
   * test hook to simulate instance crash or GC, without cleaning up zookeeper
   */
  public void forceDisconnect() {
    if (_zkclient != null) {
      _zkclient.close();
      _zkclient = null;
    }
  }

  /**
   * Connect the adapter so that it can connect and bridge events between zookeeper changes and
   * the actions that needs to be taken with them, which are implemented in the Coordinator class
   *
   */
  public void connect() {
    _zkclient = new ZkClient(_zkServers, _sessionTimeout, _connectionTimeout);

    // create a globally uniq instance name and create a live instance node in zookeeper
    _instanceName = createLiveInstanceNode();
    _log = LoggerFactory.getLogger(String.format("%s:%s", ZkAdapter.class.getName(), _instanceName));

    _log.info("Coordinator instance " + _instanceName + " is online");

    // both leader and follower needs to listen to its own instance change
    // under /{cluster}/instances/{instance}
    _assignmentList = new ZkBackedTaskListProvider();

    // start with follower state, then join leader election
    onBecomeFollower();
    joinLeaderElection();

    _log.info("Instance " + _instanceName + " is ready.");
    // populate the instance name.
  }

  private void onBecomeLeader() {
    _log.info("Instance " + _instanceName + " becomes leader");

    _datastreamList = new ZkBackedDMSDatastreamList();
    _liveInstancesProvider = new ZkBackedLiveInstanceListProvider();

    _zkclient.subscribeChildChanges(KeyBuilder.instances(_cluster), _liveInstancesProvider);

    // Load all existing tasks when we just become the new leader. This is needed
    // for resuming working on the tasks from previous sessions.
    loadAllDatastreamTasks();

    if (_listener != null) {
      _listener.onBecomeLeader();
    }
  }

  private void onBecomeFollower() {
    _log.info("Instance " + _instanceName + " becomes follower");

    if (_datastreamList != null) {
      _datastreamList.close();
      _datastreamList = null;
    }

    if (_liveInstancesProvider != null) {
      _liveInstancesProvider.close();
      _liveInstancesProvider = null;
    }

    _zkclient.unsubscribeChildChanges(KeyBuilder.instances(_cluster), _liveInstancesProvider);
  }

  /**
   *  Each instance of coordinator (and coordinator zk adapter) must participate the leader
   *  election. This method will be called when the zk connection is made, in ZkAdapter.connect() method.
   *  This is a standard implementation of the ZooKeeper leader election recipe.
   *
   *  <ul>
   *      <li>
   *          Create an ephemeral sequential znode under the leader election path. The leader election path
   *          is <i>/{cluster}/liveinstances</i>. The znode name is the sequence number starting from "00000000".
   *          The content of this znode is the hostname. After the live instance node is successfully created,
   *          we also create the matching instance znode. The matching instance znode has the path
   *          <i>/{cluster}/instances</i>, and the node name is of the format {hostname}-{sequence}.
   *          For example, "ychen1-mn1-00000000" means the instance is running on the host "ychen1-mn1" and
   *          its matching live instance node name is "00000000".
   *      </li>
   *
   *      <li>
   *           At any point in time, the znode with smallest sequence number is the current leader. If the
   *           current instance is not the leader, it watches the previous in line candidate. For example,
   *           the node "00000008" will watch the node "00000007", so that if "00000007" dies, "00000008"
   *           will get notified and try to run election.
   *      </li>
   *  </ul>
   */
  private void joinLeaderElection() {

    // get the list of current live instances
    List<String> liveInstances = _zkclient.getChildren(KeyBuilder.liveInstances(_cluster));
    Collections.sort(liveInstances);

    // find the position of the current instance in the list
    String[] nodePathParts = _instanceName.split("/");
    String nodeName = nodePathParts[nodePathParts.length - 1];
    nodeName = nodeName.substring(_hostname.length() + 1);
    int index = liveInstances.indexOf(nodeName);

    if (index < 0) {
      // only when the zookeeper session already expired by the time this adapter joins for leader election.
      // mostly because the zkclient session expiration timeout.
      _log.error("Failed to join leader election. Try reconnect the zookeeper");
      connect();
      return;
    }

    // if this instance is first in line to become leader. Check if it is already a leader.
    if (index == 0) {
      if (!_isLeader) {
        _isLeader = true;
        onBecomeLeader();
      }
      return;
    }

    // if this instance is not the first candidate to become leader, make sure to reset
    // the _isLeader status
    if (_isLeader) {
      _isLeader = false;
      onBecomeFollower();
    }

    // prevCandidate is the leader candidate that is in line before this current node
    // we only become the leader after prevCandidate goes offline
    String prevCandidate = liveInstances.get(index - 1);

    // if the prev candidate is not the current subscription, reset it
    if (!prevCandidate.equals(_currentSubscription)) {
      if (_currentSubscription != null) {
        _zkclient.unsubscribeDataChanges(_currentSubscription, _leaderElectionListener);
      }

      _currentSubscription = prevCandidate;
      _zkclient.subscribeDataChanges(KeyBuilder.liveInstance(_cluster, _currentSubscription), _leaderElectionListener);
    }

    // now double check if the previous candidate exists. If not, try election again
    boolean exists = _zkclient.exists(KeyBuilder.liveInstance(_cluster, prevCandidate), true);

    if (exists) {
      if (_isLeader) {
        _isLeader = false;
        onBecomeFollower();
      }
    } else {
      try {
        Thread.sleep(randomGenerator.nextInt(1000));
      } catch (InterruptedException ie) {
      }
      joinLeaderElection();
    }
  }

  /**
   * read all datastreams defined by Datastream Management Service. This method should only
   * be called by the Coordinator leader.
   *
   * @return list of Datastreams
   */
  public List<Datastream> getAllDatastreams() {
    if (_datastreamList == null) {
      // TODO: looks like it is possible to see this warning before the leader election finishes
      _log.warn("Instance " + _instanceName + " is not the leader but it is accessing the datastream list");
    }

    List<String> datastreamNames = _datastreamList.getDatastreamNames();
    List<Datastream> result = new ArrayList<>();

    for (String streamNode : datastreamNames) {
      String path = KeyBuilder.datastream(_cluster, streamNode);
      String content = _zkclient.ensureReadData(path);
      Datastream stream = DatastreamUtils.fromJSON(content);
      result.add(stream);
    }

    return result;
  }

  public boolean updateDatastream(Datastream datastream) {
    String path = KeyBuilder.datastream(_cluster, datastream.getName());
    if (!_zkclient.exists(path)) {
      _log.warn("trying to update znode of datastream that does not exist. Datastream name: " + datastream.getName());
      return false;
    }

    String data = DatastreamUtils.toJSON(datastream);
    _zkclient.updateDataSerialized(path, old -> data);
    return true;
  }

  /**
   * @return a list of instances include both dead and live ones.
   * Dead ones can be removed only after new assignments have
   * been fully populated by the leader Coordinator via strategies.
   */
  public List<String> getAllInstances() {
    return _zkclient.getChildren(KeyBuilder.instances(_cluster));
  }

  public List<String> getLiveInstances() {
    return _liveInstancesProvider.getLiveInstances();
  }

  /**
   * get all datastream tasks assigned to this instance
   * @param instance
   * @return
   */
  public List<String> getInstanceAssignment(String instance) {
    String path = KeyBuilder.instanceAssignments(_cluster, instance);
    return _zkclient.getChildren(path);
  }

  /**
   * When previous leader dies, we lose all the cached tasks.
   * As the current leader, we should try to load tasks from ZK.
   * This is very likely to be one time operation, so it should
   * okay to hit ZK.
   */
  private void loadAllDatastreamTasks() {
    if (_liveTaskMap.size() != 0) {
      return;
    }

    List<String> allInstances = getAllInstances();
    for (String instance : allInstances) {
      Set<DatastreamTask> taskMap = new HashSet<>();
      _liveTaskMap.put(instance, taskMap);
      List<String> assignment = getInstanceAssignment(instance);
      for (String taskName : assignment) {
        taskMap.add(getAssignedDatastreamTask(instance, taskName));
      }
    }
  }

  /**
   * Return a map from all instances to their currently assigned tasks.
   * NOTE: this might include the tasks assigned to dead instances because
   * in some strategies (eg. SIMPLE) tasks from dead instances need to
   * be handed off to another live instance without creating a new task
   * as the existing task still holds the checkpoints. If this method is
   * called after task reassignment, the returned map will not include
   * tasks hanging off of dead instances as nodes of dead instances have
   * been cleaned up after each task reassignment.
   *
   * @return a map of all existing DatastreamTasks
   */
  public Map<String, Set<DatastreamTask>> getAllAssignedDatastreamTasks() {
    _log.info("All live tasks: " + _liveTaskMap);
    return new HashMap<>(_liveTaskMap);
  }

  /**
   * given an instance name and a datastreamtask name assigned to this instance, read
   * the znode content under /{cluster}/instances/{instance}/{taskname} and return
   * an instance of DatastreamTask
   *
   * @param instance
   * @param taskName
   * @return
   */
  public DatastreamTask getAssignedDatastreamTask(String instance, String taskName) {
    String content = _zkclient.ensureReadData(KeyBuilder.instanceAssignment(_cluster, instance, taskName));
    DatastreamTaskImpl task = DatastreamTaskImpl.fromJson(content);
    String dsName = task.getDatastreamName();
    task.setZkAdapter(this);

    String dsPath = KeyBuilder.datastream(_cluster, dsName);
    if (!_zkclient.exists(dsPath)) {
      // FIXME: we should do some error handling
      String errorMessage = String.format("Missing Datastream in ZooKeeper for task={%s} instance=%s", task, instance);
      ErrorLogger.logAndThrowDatastreamRuntimeException(_log, errorMessage, null);
    }
    String dsContent = _zkclient.ensureReadData(dsPath);
    Datastream stream = DatastreamUtils.fromJSON(dsContent);
    task.setDatastream(stream);

    return task;
  }

  /**
   * Three directories need to be created for a new task:
   *
   *  - /<cluster>/instances/<instance>/<task>[JSON]
   *  - /<cluster>/connectors/<connectorType>/<task>/<config>
   *  - /<cluster>/connectors/<connectorType>/<task>/<state>
   *
   *  If any one failed, RuntimeException will be thrown.
   */
  private void addTaskNodes(String instance, DatastreamTaskImpl task) {
    String name = task.getDatastreamTaskName();

    // Must add task node under connector first because as soon as we update the
    // instance assignment node, ZkBackTaskListProvider will be notified and the
    // connector will receive onAssignmentChange() with the new task. If it tries
    // to acquire the task before the connector task node is created, this will
    // fail with NoNodeException since lock node hangs off of connector task node.
    String taskConfigPath =
        KeyBuilder.datastreamTaskConfig(_cluster, task.getConnectorType(), task.getDatastreamTaskName());
    _zkclient.ensurePath(taskConfigPath);

    // Task state
    String taskStatePath =
        KeyBuilder.datastreamTaskState(_cluster, task.getConnectorType(), task.getDatastreamTaskName());
    _zkclient.ensurePath(taskStatePath);

    String instancePath = KeyBuilder.instanceAssignment(_cluster, instance, name);
    String json = "";
    try {
      json = task.toJson();
    } catch (IOException e) {
      // This should never happen
      String errorMessage = "Failed to serialize task into JSON.";
      ErrorLogger.logAndThrowDatastreamRuntimeException(_log, errorMessage, e);
    }

    // Ensure that the instance and instance/Assignment paths are ready before writing the task
    _zkclient.ensurePath(KeyBuilder.instance(_cluster, instance));
    _zkclient.ensurePath(KeyBuilder.instanceAssignments(_cluster, instance));
    String created = _zkclient.create(instancePath, json, CreateMode.PERSISTENT);

    if (created != null && !created.isEmpty()) {
      _log.info("create zookeeper node: " + instancePath);
    } else {
      // FIXME: we should do some error handling
      String errorMessage = "failed to create zookeeper node: " + instancePath;
      ErrorLogger.logAndThrowDatastreamRuntimeException(_log, errorMessage, null);
    }
  }

  /**
   * Two nodes need to be removed for a removed task:
   *
   *  - /<cluster>/instances/<instance>/<task>[JSON]
   *  - /<cluster>/connectors/<connectorType>/<task>
   *
   *  If either failed, RuntimeException will be thrown.
   */
  private void removeTaskNodes(String instance, String name) {
    String instancePath = KeyBuilder.instanceAssignment(_cluster, instance, name);

    // NOTE: we can't remove the connector task node since it has the state (checkpoint/lock).
    // Instead, we'll keep the task node alive and remove in cleanupDeadInstanceAssignments()
    // after the assignment strategy has decided to keep or leave the task.

    // Remove the task node under instance assignment
    _zkclient.deleteRecursive(instancePath);
  }

  /**
   * update the task assignment of a given instance. This method is only called by the
   * coordinator leader. To execute the update, first retrieve the existing assignment,
   * then capture the difference, and only act on the differences. That is, add new
   * assignments, remove old assignments. For ones that didn't change, do nothing.
   * Two places will be written to:
   *
   *  - /<cluster>/instances/<instance>/<task1>,<task2>...
   *  - /<cluster>/connectors/<connectorType>/<task-name1>,<task-name2>...
   *
   * Return true if assignments are persisted in zookeeper successfully.
   *
   * @param instance
   * @param assignments
   */
  public boolean updateInstanceAssignment(String instance, List<DatastreamTask> assignments) {
    _log.info("Updating datastream tasks assigned for instance: " + instance + ", new assignments are: " + assignments);

    boolean result = true;

    // list of new assignment, names only
    Set<String> assignmentsNames = new HashSet<>();
    // map of assignment, from name to DatastreamTask for future reference
    Map<String, DatastreamTask> assignmentsMap = new HashMap<>();

    assignments.forEach(task -> {
      String name = task.getDatastreamTaskName();
      assignmentsNames.add(name);
      assignmentsMap.put(name, task);
    });

    // get the old assignment from zookeeper
    Set<String> oldAssignmentNames = new HashSet<>();
    String instancePath = KeyBuilder.instanceAssignments(_cluster, instance);
    if (_zkclient.exists(instancePath)) {
      oldAssignmentNames.addAll(_zkclient.getChildren(instancePath));
    }

    //
    // find assignment names removed
    //
    Set<String> removed = new HashSet<>(oldAssignmentNames);
    removed.removeAll(assignmentsNames);
    //
    // actually remove the znodes
    //
    if (removed.size() > 0) {
      _log.info("Instance: " + instance + ", removing assignments: " + setToString(removed));
      for (String name : removed) {
        removeTaskNodes(instance, name);
      }
    }
    //
    // find assignment named added
    //
    Set<String> added = new HashSet<>(assignmentsNames);
    added.removeAll(oldAssignmentNames);
    //
    // actually add znodes
    //
    if (added.size() > 0) {
      _log.info("Instance: " + instance + ", adding assignments: " + setToString(added));

      for (String name : added) {
        addTaskNodes(instance, (DatastreamTaskImpl) assignmentsMap.get(name));
      }
    }

    _liveTaskMap.put(instance, new HashSet<>(assignments));

    return result;
  }

  // helper method for generating human readable log message, from a set of strings to a string
  private String setToString(Set<String> list) {
    StringBuffer sb = new StringBuffer();
    sb.append("[");

    Iterator<String> it = list.iterator();
    boolean isFirst = true;

    while (it.hasNext()) {
      if (!isFirst) {
        sb.append(", ");
      } else {
        isFirst = false;
      }
      sb.append(it.next());
    }
    sb.append("]");

    return sb.toString();
  }

  // create a live instance node, in the form of a sequence number with the znode path
  // /{cluster}/liveinstances/{sequenceNuber}
  // also write the hostname as the content of the node. This allows us to map this node back
  // to a corresponding instance node with path /{cluster}/instances/{hostname}-{sequenceNumber}
  private String createLiveInstanceNode() {
    // make sure the live instance path exists
    _zkclient.ensurePath(KeyBuilder.liveInstances(_cluster));

    // default name in case of UnknownHostException
    _hostname = "UnknowHost-" + randomGenerator.nextInt(10000);

    try {
      _hostname = InetAddress.getLocalHost().getHostName();
    } catch (UnknownHostException uhe) {
      _log.error(uhe.getMessage());
    }

    //
    // create an ephemeral sequential node under /{cluster}/liveinstances for leader election
    //
    String electionPath = KeyBuilder.liveInstance(_cluster, "");
    _log.info("Creating ephemeral node " + electionPath);
    String liveInstancePath = _zkclient.create(electionPath, _hostname, CreateMode.EPHEMERAL_SEQUENTIAL);
    _liveInstanceName = liveInstancePath.substring(electionPath.length());

    //
    // create instance node /{cluster}/instance/{instanceName} for keeping instance
    // states, including instance assignments and errors
    //
    String instanceName = _hostname + "-" + _liveInstanceName;
    _zkclient.ensurePath(KeyBuilder.instance(_cluster, instanceName));
    _zkclient.ensurePath(KeyBuilder.instanceAssignments(_cluster, instanceName));
    _zkclient.ensurePath(KeyBuilder.instanceErrors(_cluster, instanceName));

    return _hostname + "-" + _liveInstanceName;
  }

  public void ensureConnectorZNode(String connectorType) {
    String path = KeyBuilder.connector(_cluster, connectorType);
    _zkclient.ensurePath(path);
  }

  /**
   * Save the error message in zookeeper under /{cluster}/instances/{instanceName}/errors
   * @param message
   */
  public void zkSaveInstanceError(String message) {
    String path = KeyBuilder.instanceErrors(_cluster, _instanceName);
    if (!_zkclient.exists(path)) {
      _log.warn("failed to persist instance error because znode does not exist:" + path);
      return;
    }

    // the coordinator is a server, so let's don't do infinite retry, log
    // error instead. The error node in zookeeper will stay only when the instance
    // is alive. Only log the first 10 errors that would be more than enough for
    // debugging the server in most cases.

    int numberOfExistingErrors = _zkclient.countChildren(path);
    if (numberOfExistingErrors < 10) {
      try {
        String errorNode = _zkclient.createPersistentSequential(path + "/", message);
        _log.info("created error node at: " + errorNode);
      } catch (RuntimeException ex) {
        _log.error("failed to create instance error node: " + path);
      }
    }
  }

  /**
   * return the znode content under /{cluster}/connectors/{connectorType}/{datastreamTask}/state
   * @param datastreamTask
   * @param key
   * @return
   */
  public String getDatastreamTaskStateForKey(DatastreamTask datastreamTask, String key) {
    String path =
        KeyBuilder.datastreamTaskStateKey(_cluster, datastreamTask.getConnectorType(),
                datastreamTask.getDatastreamTaskName(), key);
    return _zkclient.readData(path, true);
  }

  /**
   * set value for znode content at /{cluster}/connectors/{connectorType}/{datastreamTask}/state
   * @param datastreamTask
   * @param key
   * @param value
   */
  public void setDatastreamTaskStateForKey(DatastreamTask datastreamTask, String key, String value) {
    String path =
        KeyBuilder.datastreamTaskStateKey(_cluster, datastreamTask.getConnectorType(),
                datastreamTask.getDatastreamTaskName(), key);
    _zkclient.ensurePath(path);
    _zkclient.writeData(path, value);
  }

  /**
   * Remove instance assignment nodes whose instances are dead.
   * NOTE: this should only be called after the valid tasks have been
   * reassigned or safe to discard per strategy requirement.
   *
   * Coordinator is expect to cache the "current" assignment before
   * invoking the assignment strategy and pass the saved assignment
   * to us to figure out the obsolete tasks.
   *
   * @param previousAssignment instance assignment before reassignment by the strategy
   */
  public void cleanupDeadInstanceAssignments(Map<String, Set<DatastreamTask>> previousAssignment) {
    List<String> liveInstances = getLiveInstances();
    List<String> deadInstances = getAllInstances();
    deadInstances.removeAll(liveInstances);
    if (deadInstances.size() > 0) {
      _log.info("Cleaning up assignments for dead instances: " + deadInstances);

      for (String instance : deadInstances) {
        String path = KeyBuilder.instance(_cluster, instance);
        if (!_zkclient.deleteRecursive(path)) {
          // Ignore such failure for now
          _log.warn("Failed to remove assignment: " + path);
        }

        if (_liveTaskMap.containsKey(instance)) {
          _liveTaskMap.remove(instance);
        }
      }
    }

    Set<DatastreamTask> deadTasks = new HashSet<>();
    previousAssignment.values().forEach(assn -> deadTasks.addAll(assn));
    _liveTaskMap.values().forEach(assn -> deadTasks.removeAll(assn));

    if (deadTasks.size() > 0) {
      _log.info("Cleaning up deprecated connector tasks: " + deadTasks);
      for (DatastreamTask task : deadTasks) {
        String path = KeyBuilder.connectorTask(_cluster, task.getConnectorType(), task.getDatastreamTaskName());
        if (_zkclient.exists(path) && !_zkclient.deleteRecursive(path)) {
          // Ignore such failure for now
          _log.warn("Failed to remove connector task: " + path);
        }
      }
    }
  }

  public void acquireTask(DatastreamTaskImpl task, long timeoutMs) throws DatastreamException {
    String lockPath = KeyBuilder.datastreamTaskLock(_cluster, task.getConnectorType(), task.getDatastreamTaskName());
    String owner = null;
    if (_zkclient.exists(lockPath)) {
      owner = _zkclient.ensureReadData(lockPath);
      if (owner.equals(_instanceName)) {
        _log.info(String.format("%s already owns the lock on %s", _instanceName, task));
        return;
      }

      Object lock = new Object();

      File lockFile = new File(lockPath);
      String fileName = lockFile.getName();

      String taskPath = KeyBuilder.connectorTask(_cluster, task.getConnectorType(), task.getDatastreamTaskName());
      IZkChildListener listener = (parentPath, currentChilds) -> {
        if (!currentChilds.contains(fileName)) {
          synchronized (lock) {
            lock.notify();
          }
        }
      };

      try {
        _zkclient.subscribeChildChanges(taskPath, listener);

        if (_zkclient.exists(lockPath)) {
          synchronized (lock) {
            try {
              lock.wait(timeoutMs);
            } catch (InterruptedException e) {
              // Fall through if interrupted
            }
          }
        }
      } finally {
        _zkclient.unsubscribeChildChanges(taskPath, listener);
      }
    }

    if (!_zkclient.exists(lockPath)) {
      _zkclient.createEphemeral(lockPath, _instanceName);
      _log.info(String.format("%s successfully acquired the lock on %s", _instanceName, task));
    } else {
      String errorMessage = String.format("%s failed to acquire the lock on datastream task after %dms on %s, current owner: %s",
          _instanceName, timeoutMs, task, owner);
      ErrorLogger.logAndThrowDatastreamRuntimeException(_log, errorMessage, null);
    }
  }

  public void releaseTask(DatastreamTaskImpl task) {
    String lockPath = KeyBuilder.datastreamTaskLock(_cluster, task.getConnectorType(), task.getDatastreamTaskName());
    if (!_zkclient.exists(lockPath)) {
      _log.info(String.format("There is no lock on %s", task));
      return;
    }

    String owner = _zkclient.ensureReadData(lockPath);
    if (!owner.equals(_instanceName)) {
      _log.warn(String.format("%s does not have the lock on %s", _instanceName, task));
      return;
    }

    _zkclient.delete(lockPath);
    _log.info(String.format("%s successfully released the lock on %s", _instanceName, task));
  }

  /**
   * ZkAdapterListener is the observer of the observer pattern. It observes the associated ZkAdapter
   * and the methods are called when the corresponding events are fired that is concerning the
   * ZkAdapter.
   */
  public interface ZkAdapterListener {
    /**
     * onBecomeLeader() is called when this Coordinator becomes the leader. This should trigger
     * the calling of task assignment.
     */
    void onBecomeLeader();

    /**
     * onLiveInstancesChange is called when the list of live instances changed. That is, any
     * children change under zookeeper under /{cluster}/instances. This method is called
     * only when this Coordinator is the leader.
     */
    void onLiveInstancesChange();

    /**
     * onAssignmentChange is a callback method triggered when the children changed in zookeeper
     * under /{cluster}/instances/{instance-name}.
     */
    void onAssignmentChange();

    /**
     * onDatastreamChange is called when there are changes to the datastreams under
     * zookeeper path /{cluster}/datastream.
     */
    void onDatastreamChange();
  }

  /**
   * ZkBackedDMSDatastreamList
   */
  public class ZkBackedDMSDatastreamList implements IZkChildListener {
    private List<String> _datastreams = new ArrayList<>();
    private String _path;

    /**
     * default constructor, it will initiate the list by first read from zookeeper, and also setup
     * a watch on the /{cluster}/datastream tree, so it can be notified for future changes.
     */
    public ZkBackedDMSDatastreamList() {
      _path = KeyBuilder.datastreams(_cluster);
      _zkclient.ensurePath(KeyBuilder.datastreams(_cluster));
      _log.info("ZkBackedDMSDatastreamList::Subscribing to the changes under the path " + _path);
      _zkclient.subscribeChildChanges(_path, this);
      _datastreams = _zkclient.getChildren(_path, true);
    }

    public void close() {
      _log.info("ZkBackedDMSDatastreamList::Unsubscribing to the changes under the path " + _path);
      _zkclient.unsubscribeChildChanges(_path, this);
    }

    @Override
    public synchronized void handleChildChange(String parentPath, List<String> currentChildren) throws Exception {
      _log.info(String.format("ZkBackedDMSDatastreamList::Received Child change notification on the datastream list"
          + "parentPath %s,children %s", parentPath, currentChildren));
      _datastreams = currentChildren;
      if (_listener != null) {
        _listener.onDatastreamChange();
      }
    }

    public List<String> getDatastreamNames() {
      return _datastreams;
    }
  }

  /**
   * ZkBackedLiveInstanceListProvider is only used by the current leader instance. It provides a cached
   * list of current live instances, by watching the live instances tree in zookeeper. The watched path
   * is <i>/{cluster}/liveinstances</i>. Note the difference between the node names under live instances
   * znode and under instances znode: the znode for instances has the format {hostname}-{sequence}.
   * ZkBackedLiveInstanceListProvider is responsible to do the translation from live instance names
   * to instance names.
   *
   * <p>Because ZkBackedLiveInstanceListProvider abstracts the knowledge of live instances, it is
   * also responsible for cleaning up when a previously live instances go offline or crash. When
   * that happens, ZkBackedLiveInstanceListProvider will remove the corresponding instance node under
   * <i>/{cluster}/instances</i>.
   *
   * <p>When the previous leader instance goes offline itself, a new leader will be elected, and
   * the new leader is responsible for cleaning up the instance node for the previous leader. This is
   * done in the constructor ZkBackedLiveInstanceListProvider().
   */
  public class ZkBackedLiveInstanceListProvider implements IZkChildListener {
    private List<String> _liveInstances = new ArrayList<>();
    private String _path;

    public ZkBackedLiveInstanceListProvider() {
      _path = KeyBuilder.liveInstances(_cluster);
      _zkclient.ensurePath(_path);
      _log.info("ZkBackedLiveInstanceListProvider::Subscribing to the under the path " + _path);
      _zkclient.subscribeChildChanges(_path, this);
      _liveInstances = getLiveInstanceNames(_zkclient.getChildren(_path));
    }

    // translate list of node names in the form of sequence number to list of instance names
    // in the form of {hostname}-{sequence}.
    private List<String> getLiveInstanceNames(List<String> nodes) {
      List<String> liveInstances = new ArrayList<>();
      for (String n : nodes) {
        String hostname = _zkclient.ensureReadData(KeyBuilder.liveInstance(_cluster, n));
        liveInstances.add(hostname + "-" + n);
      }
      return liveInstances;
    }

    public void close() {
      _log.info("ZkBackedLiveInstanceListProvider::Unsubscribing to the under the path " + _path);
      _zkclient.unsubscribeChildChanges(_path, this);
    }

    public List<String> getLiveInstances() {
      return _liveInstances;
    }

    @Override
    public void handleChildChange(String parentPath, List<String> currentChildren) throws Exception {
      _log.info(String.format("ZkBackedLiveInstanceListProvider::Received Child change notification on the instances list "
              + "parentPath %s,children %s", parentPath, currentChildren));

      _liveInstances = getLiveInstanceNames(_zkclient.getChildren(_path));;

      if (_listener != null) {
        _listener.onLiveInstancesChange();
      }
    }
  }

  public class ZkLeaderElectionListener implements IZkDataListener {
    @Override
    public void handleDataChange(String dataPath, Object data) throws Exception {
      joinLeaderElection();
    }

    @Override
    public void handleDataDeleted(String dataPath) throws Exception {
      joinLeaderElection();
    }
  }

  /**
   * ZkBackedTaskListProvider provides informations about all DatastreamTasks existing in the cluster
   * grouped by the connector type. In addition, it notifies the listener about changes happened to
   * task node changes under the connector node.
   */
  public class ZkBackedTaskListProvider implements IZkChildListener {
    private String _path = KeyBuilder.instanceAssignments(_cluster, _instanceName);

    public ZkBackedTaskListProvider() {
      _log.info("ZkBackedTaskListProvider::Subscribing to the changes under the path " + _path);
      _zkclient.subscribeChildChanges(_path, this);
    }

    public void close() {
      _log.info("ZkBackedTaskListProvider::Unsubscribing to the changes under the path " + _path);
      _zkclient.unsubscribeChildChanges(KeyBuilder.instanceAssignments(_cluster, _instanceName), this);
    }

    @Override
    public synchronized void handleChildChange(String parentPath, List<String> currentChildren) throws Exception {
      _log.info(String.format(
          "ZkBackedTaskListProvider::Received Child change notification on the datastream task list "
              + "parentPath %s,children %s", parentPath, currentChildren));
      if (_listener != null) {
        _listener.onAssignmentChange();
      }
    }
  }
}
