/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.server.zk;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.IZkDataListener;
import org.I0Itec.zkclient.exception.ZkException;
import org.I0Itec.zkclient.exception.ZkNoNodeException;
import org.apache.zookeeper.CreateMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Strings;

import com.linkedin.datastream.common.Datastream;
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
 * │  |                  │          │└───────────────────────────────────┘    │         │───▶ onBecomeLeader*│
 * │  |- liveinstances ──┼───┐      │┌───────────────────────────────────┐    │         │   └────────────────┘
 * │  |  |- i001         │   └──────▶│ ZkBackedLiveInstanceListProvider  │    │         │   ┌───────────────────────┐
 * │  |  |- i002─────────┼┐         │└───────────────────────────────────┘    │         │───▶ onDatastreamAddOrDrop*│
 * │  |                  ││         │┌───────────────────────────────────┐    │ZkAdapter│   └───────────────────────┘
 * │  |- connectors      │└─────────▶│     ZkLeaderElectionListener      │    │Listener │   ┌────────────────────┐
 * │  |  |- Espresso ────┼───┐      │└───────────────────────────────────┘    │         │───▶ onAssignmentChange │
 * │  |  |- Oracle ──────┼──┐│      │┌───────────────────────────────────┐    │         │   └────────────────────┘
 * │                     │  └┴──────┼▶    ZkBackedDatastreamTasksMap     │    │         │   ┌───────────────────────┐
 * │                     │          │└───────────────────────────────────┘    │         │───▶ onLiveInstancesChange*│
 * │                     │          │                                         │         │   └───────────────────────┘
 * │                     │          │                                         └─────────┤   ┌────────────────────┐
 * │                     │          │                                                   │───▶ onDatastreamUpdate │
 * │                     │          │                                                   │   └────────────────────┘
 * │                     │          │                                                   │
 * └─────────────────────┘          │                                                   │
 *                                  └───────────────────────────────────────────────────┘
 *
 *
 *  Note: * Callback for leader only.
 *
 * ZkAdapter is the adapter between the Coordinator and the ZkClient. It uses ZkClient to communicate
 * with ZooKeeper, and provides a set of callbacks that allows the Coordinator to react on events like
 * leadership changes, assigment changes, and live instances changes.
 *
 * <p> ZkAdapter provide two main roles:
 * <ul>
 *     <li>ZooKeeper-backed data provider. Each of these zk-backed data provider is implemented as an embedded
 *     class. For example, {@link com.linkedin.datastream.server.zk.ZkAdapter.ZkBackedLiveInstanceListProvider}
 *     provides the current list of live instances, and its data is automatically updated when the underlying
 *     ZooKeeper data structure is updated.
 *     </li>
 *
 *     <li>Notify the observers. ZkAdapter will trigger the {@link com.linkedin.datastream.server.zk.ZkAdapter.ZkAdapterListener}
 *     callbacks based on the current state. The {@link com.linkedin.datastream.server.Coordinator} implements
 *     this interface so it can take appropriate action.
 *     </li>
 * </ul>
 *
 * <p>The ZK-backed data providers cache the data read from the corresponding ZooKeeper nodes so they can be accessed
 * without reading ZooKeeper frequently. These providers also set up the watch on these nodes so it can be notified
 * when the data changes. For example, {@link com.linkedin.datastream.server.zk.ZkAdapter.ZkBackedTaskListProvider}
 * provides the list of DatastreamTask objects that are assigned to this instance. This provider also watches the
 * znode /{cluster}/instances/{instanceName} for children changes, and automatically refreshes the cached values.
 *
 * @see com.linkedin.datastream.server.Coordinator
 * @see ZkClient
 */

public class ZkAdapter {
  private static final Logger LOG = LoggerFactory.getLogger(ZkAdapter.class);

  private final String _defaultTransportProviderName;

  private String _zkServers;
  private String _cluster;
  private int _sessionTimeout;
  private int _connectionTimeout;
  private ZkClient _zkclient;

  private String _instanceName;
  private String _liveInstanceName;
  private String _hostname;

  private volatile boolean _isLeader = false;
  private ZkAdapterListener _listener;

  // the current znode this node is listening to
  private String _currentSubscription = null;

  private Random randomGenerator = new Random();

  private ZkLeaderElectionListener _leaderElectionListener = new ZkLeaderElectionListener();
  private ZkBackedTaskListProvider _assignmentList = null;

  // only the leader should maintain this list and listen to the changes of live instances
  private ZkBackedDMSDatastreamList _datastreamList = null;
  private ZkBackedLiveInstanceListProvider _liveInstancesProvider = null;

  // Cache all live DatastreamTasks per instance for assignment strategy
  private Map<String, Set<DatastreamTask>> _liveTaskMap = new HashMap<>();

  public ZkAdapter(String zkServers, String cluster, String defaultTransportProviderName, int sessionTimeout,
      int connectionTimeout, ZkAdapterListener listener) {
    _zkServers = zkServers;
    _cluster = cluster;
    _sessionTimeout = sessionTimeout;
    _connectionTimeout = connectionTimeout;
    _listener = listener;
    _defaultTransportProviderName = defaultTransportProviderName;
  }

  public synchronized boolean isLeader() {
    return _isLeader;
  }

  public String getInstanceName() {
    return _instanceName;
  }

  /**
   * gracefully disconnect from Zookeper, clean up znodes
   */
  public void disconnect() {

    if (_zkclient != null) {
      try {
        // remove the liveinstance node
        String liveInstancePath = KeyBuilder.liveInstance(_cluster, _liveInstanceName);
        LOG.info("deleting live instance node: " + liveInstancePath);
        _zkclient.delete(liveInstancePath);

        // NOTE: we should not delete the instance node which still holds the
        // assigned tasks. Coordinator will call cleanupDeadInstanceAssignments
        // to do an ad-hoc cleanup once the tasks haven been properly handled
        // per the strategy (reassign or discard).
      } catch (ZkException zke) {
        // do nothing, best effort clean up
      } finally {
        if (_assignmentList != null) {
          _assignmentList.close();
          _assignmentList = null;
        }
        _zkclient.close();
        _zkclient = null;
      }
    }
    // isLeader will be reinitialized when we reconnect
  }

  /**
   * Connect the adapter so that it can connect and bridge events between Zookeper changes and
   * the actions that need to be taken with them, which are implemented in the Coordinator class
   *
   */
  public void connect() {
    _zkclient = new ZkClient(_zkServers, _sessionTimeout, _connectionTimeout);

    // create a globally uniq instance name and create a live instance node in Zookeper
    _instanceName = createLiveInstanceNode();

    LOG.info("Coordinator instance " + _instanceName + " is online");

    // both leader and follower needs to listen to its own instance change
    // under /{cluster}/instances/{instance}
    _assignmentList = new ZkBackedTaskListProvider(_cluster, _instanceName);

    // start with follower state, then join leader election
    onBecomeFollower();
    joinLeaderElection();

    LOG.info("Instance " + _instanceName + " is ready.");
    // populate the instance name.
  }

  private void onBecomeLeader() {
    LOG.info("Instance " + _instanceName + " becomes leader");

    _datastreamList = new ZkBackedDMSDatastreamList();
    _liveInstancesProvider = new ZkBackedLiveInstanceListProvider();

    // Load all existing tasks when we just become the new leader. This is needed
    // for resuming working on the tasks from previous sessions.
    loadAllDatastreamTasks();

    _isLeader = true;

    if (_listener != null) {
      _listener.onBecomeLeader();
    }
  }

  private void onBecomeFollower() {
    LOG.info("Instance " + _instanceName + " becomes follower");

    if (_datastreamList != null) {
      _datastreamList.close();
      _datastreamList = null;
    }

    if (_liveInstancesProvider != null) {
      _liveInstancesProvider.close();
      _liveInstancesProvider = null;
    }

    _isLeader = false;
  }

  /**
   *  Each instance of coordinator (and coordinator zk adapter) must participate in the leader
   *  election. This method will be called when the zk connection is made, in ZkAdapter.connect() method.
   *  This is a standard implementation of ZooKeeper leader election recipe.
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
      // only when the Zookeper session already expired by the time this adapter joins for leader election.
      // mostly because the zkclient session expiration timeout.
      LOG.error("Failed to join leader election. Try reconnect the zookeeper");
      connect();
      return;
    }

    // if this instance is first in line to become leader. Check if it is already a leader.
    if (index == 0) {
      if (!_isLeader) {
        onBecomeLeader();
      }
      return;
    }

    // This instance is not the first candidate to become leader.
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
      // if this instance is not the first candidate to become leader, make sure to reset
      // the _isLeader status
      if (_isLeader) {
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

  public boolean updateDatastream(Datastream datastream) {
    String path = KeyBuilder.datastream(_cluster, datastream.getName());
    if (!_zkclient.exists(path)) {
      LOG.warn("trying to update znode of datastream that does not exist. Datastream name: " + datastream.getName());
      return false;
    }

    String data = DatastreamUtils.toJSON(datastream);
    _zkclient.updateDataSerialized(path, old -> data);
    return true;
  }

  public void deleteTasksWithPrefix(Set<String> connectors, String taskPrefix) {
    Set<String> tasksToDelete = _liveTaskMap.values()
        .stream()
        .flatMap(Collection::stream)
        .filter(x -> x.getTaskPrefix().equals(taskPrefix))
        .map(DatastreamTask::getDatastreamTaskName)
        .collect(Collectors.toSet());

    for (String connector : connectors) {
      Set<String> allTasks = new HashSet<>(_zkclient.getChildren(KeyBuilder.connector(_cluster, connector)));
      List<String> deadTasks = allTasks.stream().filter(tasksToDelete::contains).collect(Collectors.toList());

      if (deadTasks.size() > 0) {
        LOG.info("Cleaning up deprecated connector tasks: {} for connector: {}", deadTasks, connector);
        for (String task : deadTasks) {
          deleteConnectorTask(connector, task);
        }
      }
    }
  }

  private void deleteConnectorTask(String connector, String taskName) {
    LOG.info("Trying to delete task" + taskName);
    String path = KeyBuilder.connectorTask(_cluster, connector, taskName);
    if (_zkclient.exists(path) && !_zkclient.deleteRecursive(path)) {
      // Ignore such failure for now
      LOG.warn("Failed to remove connector task: " + path);
    }
  }

  public void deleteDatastream(String datastreamName) {
    String path = KeyBuilder.datastream(_cluster, datastreamName);

    if (!_zkclient.exists(path)) {
      LOG.warn("trying to delete znode of datastream that does not exist. Datastream name: " + datastreamName);
      return;
    }

    LOG.info("Deleting the zk path {} ", path);
    // Pipeline could have created more nodes under datastream node. Delete all associated state with deleteRecursive
    _zkclient.deleteRecursive(path);
  }

  /**
   * @return a list of instances including both dead and live ones.
   * Dead ones can be removed only after new assignments have
   * been fully populated by the leader Coordinator via strategies.
   */
  public List<String> getAllInstances() {
    String path = KeyBuilder.instances(_cluster);
    _zkclient.ensurePath(path);
    return _zkclient.getChildren(path);
  }

  /**
   * Touch all assignment nodes for every instance, so that all instances get notified that some datastreams
   * get updated.
   */
  public void touchAllInstanceAssignments() {
    List<String> allInstances = getAllInstances();
    LOG.info("About to touch all instances' assignments node. instances = {}", allInstances);
    // since all the requests below talk to the same zk server, we don't benefit a lot from parallelism
    for (String instance : allInstances) {
      // Ensure that the instance and instance/Assignment paths are ready before writing the task
      if (_zkclient.exists(KeyBuilder.instance(_cluster, instance)) && _zkclient.exists(
          KeyBuilder.instanceAssignments(_cluster, instance))) {
        try {
          _zkclient.writeData(KeyBuilder.instanceAssignments(_cluster, instance),
              String.valueOf(System.currentTimeMillis()));
        } catch (Exception e) {
          // we don't need to do an atomic update; if the node gets update by others somehow or get deleted by
          // leader, it's ok to ignore the failure
          LOG.warn("Failed to touch the assignment node for instance " + instance, e);
        }
      }
    }
  }

  public List<String> getLiveInstances() {
    return _liveInstancesProvider.getLiveInstances();
  }

  /**
   * get all datastream tasks assigned to this instance
   */
  public List<String> getInstanceAssignment(String instance) {
    String path = KeyBuilder.instanceAssignments(_cluster, instance);
    if (!_zkclient.exists(path)) {
      return Collections.emptyList();
    }
    return _zkclient.getChildren(path);
  }

  /**
   * When the previous leader dies, we lose all the cached tasks.
   * As the current leader, we should try to load tasks from ZK.
   * This is very likely to be one time operation, so it should be
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
    LOG.info("All live tasks: " + _liveTaskMap);
    return new HashMap<>(_liveTaskMap);
  }

  /**
   * given an instance name and a datastreamtask name assigned to this instance, read
   * the znode content under /{cluster}/instances/{instance}/{taskname} and return
   * an instance of DatastreamTask
   *
   * @return null if task node does not exist or inaccessible
   */
  public DatastreamTaskImpl getAssignedDatastreamTask(String instance, String taskName) {
    try {
      String content = _zkclient.ensureReadData(KeyBuilder.instanceAssignment(_cluster, instance, taskName));
      DatastreamTaskImpl task = DatastreamTaskImpl.fromJson(content);
      if (Strings.isNullOrEmpty(task.getTaskPrefix())) {
        task.setTaskPrefix(parseTaskPrefix(task.getDatastreamTaskName()));
      }

      if (Strings.isNullOrEmpty(task.getTransportProviderName())) {
        task.setTransportProviderName(_defaultTransportProviderName);
      }

      task.setZkAdapter(this);
      return task;
    } catch (ZkNoNodeException e) {
      // This can occur if there is another task assignment change in the middle of
      // handleAssignmentChange and some tasks are unassigned to the current
      // instance. In this case, we would get such exception. This is tolerable
      // because we should be receiving another AssignmentChange event right after
      // then we can dispatch the tasks based on the latest assignment data.
      LOG.warn("ZNode does not exist for instance={}, task={}, ignoring the task.", instance, taskName);
      return null;
    }
  }

  private String parseTaskPrefix(String datastreamTaskName) {
    return datastreamTaskName.substring(0, datastreamTaskName.lastIndexOf("_"));
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
    LOG.info("Adding Task Node: " + instance + ", task: " + task);
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
      ErrorLogger.logAndThrowDatastreamRuntimeException(LOG, errorMessage, e);
    }

    // Ensure that the instance and instance/Assignment paths are ready before writing the task
    _zkclient.ensurePath(KeyBuilder.instance(_cluster, instance));
    _zkclient.ensurePath(KeyBuilder.instanceAssignments(_cluster, instance));
    String created = _zkclient.create(instancePath, json, CreateMode.PERSISTENT);

    if (created != null && !created.isEmpty()) {
      LOG.info("create zookeeper node: " + instancePath);
    } else {
      // FIXME: we should do some error handling
      String errorMessage = "failed to create zookeeper node: " + instancePath;
      ErrorLogger.logAndThrowDatastreamRuntimeException(LOG, errorMessage, null);
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
    LOG.info("Removing Task Node: " + instance + ", task: " + name);
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
   */
  public void updateAllAssignments(Map<String, List<DatastreamTask>> assignmentsByInstance) {
    // map of task name to DatastreamTask for future reference
    Map<String, DatastreamTask> assignmentsMap = assignmentsByInstance.values()
        .stream()
        .flatMap(List::stream)
        .collect(Collectors.toMap(DatastreamTask::getDatastreamTaskName, Function.identity()));

    // For each instance, find the nodes to add and remove.
    Map<String, Set<String>> nodesToRemove = new HashMap<>();
    Map<String, Set<String>> nodesToAdd = new HashMap<>();
    diffAssignmentNodes(assignmentsByInstance, nodesToRemove, nodesToAdd);

    // Add the new tasks znodes.
    // We need to add the nodes BEFORE removing the old ones, to avoid tasks loss in case of server crash.
    // In case of crash, the new leader will remove duplicate tasks when updating the assignments.
    for (String instance : nodesToAdd.keySet()) {
      Set<String> added = nodesToAdd.get(instance);
      if (added.size() > 0) {
        LOG.info("Instance: {}, adding assignments: {}", instance, added);
        for (String name : added) {
          addTaskNodes(instance, (DatastreamTaskImpl) assignmentsMap.get(name));
        }
      }
    }

    // Second remove the old tasks znodes.
    for (String instance : nodesToRemove.keySet()) {
      Set<String> removed = nodesToRemove.get(instance);
      if (removed.size() > 0) {
        LOG.info("Instance: {}, removing assignments: ", instance, removed);
        for (String name : removed) {
          removeTaskNodes(instance, name);
        }
      }
    }

    // Finally, Save the new assignments in the cache.
    _liveTaskMap = new HashMap<>();
    for (String instance : nodesToAdd.keySet()) {
      _liveTaskMap.put(instance, new HashSet<>(assignmentsByInstance.get(instance)));
    }
  }

  /**
   * Compare the current assignment with the new assignment, and update the list of nodes
   * to add and remove per instance.
   */
  private void diffAssignmentNodes(Map<String, List<DatastreamTask>> assignmentsByInstance,
      Map<String, Set<String>> nodesToRemove, Map<String, Set<String>> nodesToAdd) {
    for (String instance : assignmentsByInstance.keySet()) {
      // list of new assignment, names only
      Set<String> assignmentsNames = assignmentsByInstance.get(instance)
          .stream()
          .map(DatastreamTask::getDatastreamTaskName)
          .collect(Collectors.toSet());

      // get the old assignment from Zookeper
      Set<String> oldAssignmentNames = new HashSet<>();
      String instancePath = KeyBuilder.instanceAssignments(_cluster, instance);
      if (_zkclient.exists(instancePath)) {
        oldAssignmentNames.addAll(_zkclient.getChildren(instancePath));
      }

      //
      // find assignments removed
      //
      Set<String> removed = new HashSet<>(oldAssignmentNames);
      removed.removeAll(assignmentsNames);
      nodesToRemove.put(instance, removed);

      //
      // find assignments added
      //
      Set<String> added = new HashSet<>(assignmentsNames);
      added.removeAll(oldAssignmentNames);
      nodesToAdd.put(instance, added);
    }
  }

  // create a live instance node, in the form of a sequence number with the znode path
  // /{cluster}/liveinstances/{sequenceNumber}
  // also write the hostname as the content of the node. This allows us to map this node back
  // to a corresponding instance node with path /{cluster}/instances/{hostname}-{sequenceNumber}
  private String createLiveInstanceNode() {
    // make sure the live instance path exists
    _zkclient.ensurePath(KeyBuilder.liveInstances(_cluster));

    // default name in case of UnknownHostException
    _hostname = "UnknownHost-" + randomGenerator.nextInt(10000);

    try {
      _hostname = InetAddress.getLocalHost().getHostName();
    } catch (UnknownHostException uhe) {
      LOG.error(uhe.getMessage());
    }

    //
    // create an ephemeral sequential node under /{cluster}/liveinstances for leader election
    //
    String electionPath = KeyBuilder.liveInstance(_cluster, "");
    LOG.info("Creating ephemeral node on path: {}", electionPath);
    String liveInstancePath = _zkclient.create(electionPath, _hostname, CreateMode.EPHEMERAL_SEQUENTIAL);
    _liveInstanceName = liveInstancePath.substring(electionPath.length());
    LOG.info("Getting live instance name as: {}", _liveInstanceName);

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
   * Save the error message in Zookeper under /{cluster}/instances/{instanceName}/errors
   */
  public void zkSaveInstanceError(String message) {
    String path = KeyBuilder.instanceErrors(_cluster, _instanceName);
    if (!_zkclient.exists(path)) {
      LOG.warn("failed to persist instance error because znode does not exist:" + path);
      return;
    }

    // the coordinator is a server, so let's don't do infinite retry, log
    // error instead. The error node in Zookeper will stay only when the instance
    // is alive. Only log the first 10 errors that would be more than enough for
    // debugging the server in most cases.

    int numberOfExistingErrors = _zkclient.countChildren(path);
    if (numberOfExistingErrors < 10) {
      try {
        String errorNode = _zkclient.createPersistentSequential(path + "/", message);
        LOG.info("created error node at: " + errorNode);
      } catch (RuntimeException ex) {
        LOG.error("failed to create instance error node: " + path);
      }
    }
  }

  /**
   * return the znode content under /{cluster}/connectors/{connectorType}/{datastreamTask}/state
   */
  public String getDatastreamTaskStateForKey(DatastreamTask datastreamTask, String key) {
    String path = KeyBuilder.datastreamTaskStateKey(_cluster, datastreamTask.getConnectorType(),
        datastreamTask.getDatastreamTaskName(), key);
    return _zkclient.readData(path, true);
  }

  /**
   * set value for znode content at /{cluster}/connectors/{connectorType}/{datastreamTask}/state
   */
  public void setDatastreamTaskStateForKey(DatastreamTask datastreamTask, String key, String value) {
    String path = KeyBuilder.datastreamTaskStateKey(_cluster, datastreamTask.getConnectorType(),
        datastreamTask.getDatastreamTaskName(), key);
    _zkclient.ensurePath(path);
    _zkclient.writeData(path, value);
  }

  /**
   * Remove instance assignment nodes whose instances are dead.
   * NOTE: this should only be called after the valid tasks have been
   * reassigned or safe to discard per strategy requirement.
   *
   * Coordinator is expected to cache the "current" assignment before
   * invoking the assignment strategy and pass the saved assignment
   * to us to figure out the obsolete tasks.
   */
  public void cleanupDeadInstanceAssignments(List<String> liveInstances) {
    List<String> deadInstances = getAllInstances();
    deadInstances.removeAll(liveInstances);
    if (deadInstances.size() > 0) {
      LOG.info("Cleaning up assignments for dead instances: " + deadInstances);

      for (String instance : deadInstances) {
        String path = KeyBuilder.instance(_cluster, instance);
        LOG.info("Deleting zk path recursively: " + path);
        if (!_zkclient.deleteRecursive(path)) {
          // Ignore such failure for now
          LOG.warn("Failed to remove zk path: {} Very likely that the zk node doesn't exist anymore", path);
        }

        if (_liveTaskMap.containsKey(instance)) {
          _liveTaskMap.remove(instance);
        }
      }
    }
  }

  /**
   * New assignment may not contain all the tasks from the previous assignment, This means that the diff of the
   * tasks between the new and old assignment are not used any more which can be deleted.
   * @param previousAssignmentByInstance previous task assignment
   * @param newAssignmentsByInstance new task assignment.
   */
  public void cleanupOldUnusedTasks(Map<String, Set<DatastreamTask>> previousAssignmentByInstance,
      Map<String, List<DatastreamTask>> newAssignmentsByInstance) {

    Set<DatastreamTask> newTasks =
        newAssignmentsByInstance.values().stream().flatMap(Collection::stream).collect(Collectors.toSet());
    Set<DatastreamTask> oldTasks =
        previousAssignmentByInstance.values().stream().flatMap(Collection::stream).collect(Collectors.toSet());
    List<DatastreamTask> unusedTasks =
        oldTasks.stream().filter(x -> !newTasks.contains(x)).collect(Collectors.toList());
    LOG.warn("Deleting the unused tasks {} found between previous {} and new assignment {}. ", unusedTasks,
        previousAssignmentByInstance, newAssignmentsByInstance);

    // Delete the connector tasks.
    unusedTasks.forEach(t -> deleteConnectorTask(t.getConnectorType(), t.getDatastreamTaskName()));
  }

  private void waitForTaskRelease(DatastreamTask task, long timeoutMs, String lockPath) {
    // Latch == 1 means task is busy (still held by the previous owner)
    CountDownLatch busyLatch = new CountDownLatch(1);

    String lockNode = lockPath.substring(lockPath.lastIndexOf('/') + 1);
    String taskPath = KeyBuilder.connectorTask(_cluster, task.getConnectorType(), task.getDatastreamTaskName());

    if (_zkclient.exists(lockPath)) {
      IZkChildListener listener = (parentPath, currentChildren) -> {
        if (!currentChildren.contains(lockNode)) {
          busyLatch.countDown();
        }
      };

      try {
        _zkclient.subscribeChildChanges(taskPath, listener);
        busyLatch.await(timeoutMs, TimeUnit.MILLISECONDS);
      } catch (InterruptedException e) {
        LOG.warn("Unexpectedly interrupted during task acquire.", e);
      } finally {
        _zkclient.unsubscribeChildChanges(taskPath, listener);
      }
    }
  }

  public void acquireTask(DatastreamTaskImpl task, Duration timeout) {
    String lockPath = KeyBuilder.datastreamTaskLock(_cluster, task.getConnectorType(), task.getDatastreamTaskName());
    String owner = null;
    if (_zkclient.exists(lockPath)) {
      owner = _zkclient.ensureReadData(lockPath);
      if (owner.equals(_instanceName)) {
        LOG.info("{} already owns the lock on {}", _instanceName, task);
        return;
      }

      waitForTaskRelease(task, timeout.toMillis(), lockPath);
    }

    if (!_zkclient.exists(lockPath)) {
      _zkclient.createEphemeral(lockPath, _instanceName);
      LOG.info("{} successfully acquired the lock on {}", _instanceName, task);
    } else {
      String msg = String.format("%s failed to acquire task %s in %dms, current owner: %s", _instanceName, task,
          timeout.toMillis(), owner);
      ErrorLogger.logAndThrowDatastreamRuntimeException(LOG, msg, null);
    }
  }

  public void releaseTask(DatastreamTaskImpl task) {
    String lockPath = KeyBuilder.datastreamTaskLock(_cluster, task.getConnectorType(), task.getDatastreamTaskName());
    if (!_zkclient.exists(lockPath)) {
      LOG.info("There is no lock on {}", task);
      return;
    }

    String owner = _zkclient.ensureReadData(lockPath);
    if (!owner.equals(_instanceName)) {
      LOG.warn("{} does not have the lock on {}", _instanceName, task);
      return;
    }

    _zkclient.delete(lockPath);
    LOG.info("{} successfully released the lock on {}", _instanceName, task);
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
     * children change under Zookeper under /{cluster}/instances. This method is called
     * only when this Coordinator is the leader.
     */
    void onLiveInstancesChange();

    /**
     * onAssignmentChange is a callback method triggered when the children changed in Zookeper
     * under /{cluster}/instances/{instance-name}.
     */
    void onAssignmentChange();

    /**
     * onDatastreamAddOrDrop is called when there are changes to the datastreams under
     * Zookeper path /{cluster}/dms. This method is called only when the Coordinator
     * is the leader.
     */
    void onDatastreamAddOrDrop();

    /**
     * onDatastreamUpdate is triggered when the /{cluster}/instances/{instance-name}/assignments
     * node gets updated, which will happen after a datastream update
     */
    void onDatastreamUpdate();
  }

  /**
   * ZkBackedDMSDatastreamList
   */
  public class ZkBackedDMSDatastreamList implements IZkChildListener, IZkDataListener {
    private String _path;

    /**
     * default constructor, it will initiate the list by first read from Zookeper, and also set up
     * a watch on the /{cluster}/dms tree, so it can be notified for future changes.
     */
    public ZkBackedDMSDatastreamList() {
      _path = KeyBuilder.datastreams(_cluster);
      _zkclient.ensurePath(KeyBuilder.datastreams(_cluster));
      LOG.info("ZkBackedDMSDatastreamList::Subscribing to the changes under the path " + _path);
      _zkclient.subscribeChildChanges(_path, this);
      _zkclient.subscribeDataChanges(_path, this);
    }

    public void close() {
      LOG.info("ZkBackedDMSDatastreamList::Unsubscribing to the changes under the path " + _path);
      _zkclient.unsubscribeChildChanges(_path, this);
    }

    @Override
    public synchronized void handleChildChange(String parentPath, List<String> currentChildren) throws Exception {
      LOG.info(String.format("ZkBackedDMSDatastreamList::Received Child change notification on the datastream list"
          + "parentPath %s,children %s", parentPath, currentChildren));
      if (_listener != null && ZkAdapter.this.isLeader()) {
        _listener.onDatastreamAddOrDrop();
      }
    }

    // Triggered when the /dms is updated. The dms node is updated when someone wants to manually trigger a reassignment
    // due to datastream add or delete.
    @Override
    public void handleDataChange(String dataPath, Object data) throws Exception {
      LOG.info("ZkBackedDMSDatastreamList::Received Data change notification on the path {}, data {}.",
          dataPath, data.toString());
      if (_listener != null) {
        _listener.onDatastreamAddOrDrop();
      }
    }

    // Triggered when the /dms is deleted. This can never happen unless someone is deleting the cluster.
    @Override
    public void handleDataDeleted(String dataPath) throws Exception {
      //
    }
  }

  /**
   * ZkBackedLiveInstanceListProvider is only used by the current leader instance. It provides a cached
   * list of current live instances, by watching the live instances tree in ZooKeeper. The watched path
   * is <i>/{cluster}/liveinstances</i>. Note the difference between the node names under live instances
   * znode and under instances znode: the znode for instances has the format {hostname}-{sequence}.
   * ZkBackedLiveInstanceListProvider is responsible for translating from live instance name to instance
   * names.
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
      LOG.info("ZkBackedLiveInstanceListProvider::Subscribing to the under the path " + _path);
      _zkclient.subscribeChildChanges(_path, this);
      _liveInstances = getLiveInstanceNames(_zkclient.getChildren(_path));
    }

    // translate list of node names in the form of sequence number to list of instance names
    // in the form of {hostname}-{sequence}.
    private List<String> getLiveInstanceNames(List<String> nodes) {
      List<String> liveInstances = new ArrayList<>();
      for (String n : nodes) {
        String hostname = _zkclient.ensureReadData(KeyBuilder.liveInstance(_cluster, n));
        if (hostname != null) {
          // hostname can be null if a node dies immediately after reading all live instances
          LOG.error("Node {} is dead. Likely cause it dies after reading list of nodes.", n);
          liveInstances.add(hostname + "-" + n);
        }
      }
      return liveInstances;
    }

    public void close() {
      LOG.info("ZkBackedLiveInstanceListProvider::Unsubscribing to the under the path " + _path);
      _zkclient.unsubscribeChildChanges(_path, this);
    }

    public List<String> getLiveInstances() {
      return _liveInstances;
    }

    @Override
    public void handleChildChange(String parentPath, List<String> currentChildren) throws Exception {
      LOG.info(String.format(
          "ZkBackedLiveInstanceListProvider::Received Child change notification on the instances list "
              + "parentPath %s,children %s", parentPath, currentChildren));

      _liveInstances = getLiveInstanceNames(_zkclient.getChildren(_path));

      if (_listener != null && ZkAdapter.this.isLeader()) {
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
   * ZkBackedTaskListProvider provides information about all DatastreamTasks existing in the cluster
   * grouped by the connector type. In addition, it notifies the listener about changes that happened
   * to task node changes under the connector node.
   */
  public class ZkBackedTaskListProvider implements IZkChildListener, IZkDataListener {
    private final String _path;

    public ZkBackedTaskListProvider(String cluster, String instanceName) {
      _path = KeyBuilder.instanceAssignments(cluster, instanceName);
      LOG.info("ZkBackedTaskListProvider::Subscribing to the changes under the path " + _path);
      _zkclient.subscribeChildChanges(_path, this);
      _zkclient.subscribeDataChanges(_path, this);
    }

    public void close() {
      LOG.info("ZkBackedTaskListProvider::Unsubscribing to the changes under the path " + _path);
      _zkclient.unsubscribeChildChanges(KeyBuilder.instanceAssignments(_cluster, _instanceName), this);
    }

    @Override
    public synchronized void handleChildChange(String parentPath, List<String> currentChildren) throws Exception {
      LOG.info(String.format(
          "ZkBackedTaskListProvider::Received Child change notification on the datastream task list "
              + "parentPath %s,children %s", parentPath, currentChildren));
      if (_listener != null) {
        _listener.onAssignmentChange();
      }
    }

    // Triggered when the /assignments is updated. We want to handle this when the datastreams behind the tasks get
    // updated, but the list of tasks may remain the same
    @Override
    public void handleDataChange(String dataPath, Object data) throws Exception {
      LOG.info("ZkBackedTaskListProvider::Received Data change notification on the path {}, data {}.", dataPath, data);
      if (_listener != null && data != null && !data.toString().isEmpty()) {
        // only care about the data change when there is an update in the data node
        _listener.onDatastreamUpdate();
      }
    }

    @Override
    public void handleDataDeleted(String dataPath) throws Exception {
      // do nothing
    }
  }
}
