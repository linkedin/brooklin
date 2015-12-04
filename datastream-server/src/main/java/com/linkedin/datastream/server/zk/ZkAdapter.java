package com.linkedin.datastream.server.zk;

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

import com.linkedin.datastream.common.Datastream;
import com.linkedin.datastream.common.DatastreamJSonUtil;
import com.linkedin.datastream.server.DatastreamTask;
import com.linkedin.datastream.server.DatastreamTaskImpl;

import org.I0Itec.zkclient.IZkChildListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.I0Itec.zkclient.IZkDataListener;
import org.I0Itec.zkclient.exception.ZkException;
import org.apache.zookeeper.CreateMode;


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
  private static final Logger LOG = LoggerFactory.getLogger(ZkAdapter.class.getName());

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
  private ZkBackedDatastreamTasksMap _datastreamMap = null;

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
    return _isLeader == true;
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
        LOG.info("deleting live instance node: " + liveInstancePath);
        _zkclient.delete(liveInstancePath);

        String instancePath = KeyBuilder.instance(_cluster, _instanceName);
        LOG.info("deleting instance node: " + instancePath);
        _zkclient.deleteRecursive(KeyBuilder.instance(_cluster, _instanceName));
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
    LOG.info("Coordinator instance " + _instanceName + " is online");

    // both leader and follower needs to listen to its own instance change
    // under /{cluster}/instances/{instance}
    _assignmentList = new ZkBackedTaskListProvider();
    // each instance will need the full map of all datastream tasks
    _datastreamMap = new ZkBackedDatastreamTasksMap();

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

    if (_listener != null) {
      _listener.onBecomeLeader();
    }

    _zkclient.subscribeChildChanges(KeyBuilder.instances(_cluster), _liveInstancesProvider);
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
      LOG.error("Failed to join leader election. Try reconnect the zookeeper");
      connect();
      return;
    }

    // if this instance is first in line to become leader. Check if it is already a leader.
    if (index == 0) {
      if (_isLeader != true) {
        _isLeader = true;
        onBecomeLeader();
      }
      return;
    }

    // if this instance is not the first candidate to become leader, make sure to reset
    // the _isLeader status
    if (_isLeader == true) {
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
      if (_isLeader == true) {
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
      LOG.warn("Instance " + _instanceName + " is not the leader but it is accessing the datastream list");
    }

    List<String> datastreamNames = _datastreamList.getDatastreamNames();
    List<Datastream> result = new ArrayList<>();

    for (String streamNode : datastreamNames) {
      String path = KeyBuilder.datastream(_cluster, streamNode);
      String content = _zkclient.ensureReadData(path);
      Datastream stream = DatastreamJSonUtil.getDatastreamFromJsonString(content);
      result.add(stream);
    }

    return result;
  }

  public boolean updateDatastream(Datastream datastream) {
    String path = KeyBuilder.datastream(_cluster, datastream.getName());
    if (!_zkclient.exists(path)) {
      LOG.warn("trying to update znode of datastream that does not exist. Datastream name: " + datastream.getName());
      return false;
    }

    String data = DatastreamJSonUtil.getJSonStringFromDatastream(datastream);
    _zkclient.updateDataSerialized(path, old -> data);
    return true;
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

    String dsContent = _zkclient.ensureReadData(KeyBuilder.datastream(_cluster, dsName));
    Datastream stream = DatastreamJSonUtil.getDatastreamFromJsonString(dsContent);
    task.setDatastream(stream);
    task.setZkAdapter(this);
    return task;
  }

  /**
   * update the task assignment of a given instance. This method is only called by the
   * coordinator leader. To execute the update, first retrieve the existing assignment,
   * then capture the difference, and only act on the differences. That is, add new
   * assignments, remove old assignments. For ones that didn't change, do nothing.
   *
   * Return true if assignments are persisted in zookeeper successfully.
   *
   * @param instance
   * @param assignments
   */
  public boolean updateInstanceAssignment(String instance, List<DatastreamTask> assignments) {
    LOG.info("Updating datastream tasks assigned for instance: " + instance + ", new assignments are: " + assignments);

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
    Set<String> oldAssignmentNames;
    try {
      oldAssignmentNames = new HashSet<>(_zkclient.getChildren(KeyBuilder.instanceAssignments(_cluster, instance)));
    } catch (ZkException zke) {
      // in case the instance is already cleaned up at this moment. We should not proceed
      // instead, get into retry to recalculate the assignment
      return false;
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
      LOG.info("Instance: " + instance + ", removing assignments: " + setToString(removed));
      for (String name : removed) {
        String path = KeyBuilder.instanceAssignment(_cluster, instance, name);
        boolean deleted = _zkclient.delete(path);
        if (deleted) {
          LOG.info("deleted zookeeper node: " + path);
        } else if (_zkclient.exists(path)) {
          LOG.warn("failed to delete zookeeper node: " + path);
          result = false;
        }
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
      LOG.info("Instance: " + instance + ", adding assignments: " + setToString(added));

      for (String name : added) {
        DatastreamTaskImpl task = (DatastreamTaskImpl) assignmentsMap.get(name);
        String path = KeyBuilder.instanceAssignment(_cluster, instance, name);
        try {
          String created = _zkclient.create(path, task.toJson(), CreateMode.PERSISTENT);
          if (created != null && !created.isEmpty()) {
            LOG.info("create zookeeper node: " + path);
          } else {
            LOG.warn("failed to create zookeeper node: " + path);
          }
        } catch (IOException e) {
          // We should never get here. If we do, need to fix it with retry logic
          LOG.warn("Failed to assign task [" + name + "] to instance " + instance + ", error: " + e.getMessage());
          result = false;
        }
      }
    }

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
      LOG.error(uhe.getMessage());
    }

    //
    // create an ephemeral sequential node under /{cluster}/liveinstances for leader election
    //
    String electionPath = KeyBuilder.liveInstance(_cluster, "");
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
      LOG.warn("failed to persist instance error because znode does not exist:" + path);
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
        LOG.info("created error node at: " + errorNode);
      } catch (RuntimeException ex) {
        LOG.error("failed to create instance error node: " + path);
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
        KeyBuilder.datastreamTaskStateKey(_cluster, datastreamTask.getDatastream().getConnectorType(),
            datastreamTask.getDatastream().getName(), datastreamTask.getId(), key);
    return _zkclient.readData(path, true);
  }

  /**
   * set value for znode content at /{cluster}/connectors/{connectorType}/{datastreamTask}/state
   * @param datastreamTask
   * @param key
   * @param value
   */
  public void setDatastreamTaskStateForKey(DatastreamTask datastreamTask, String key, String value) {
    Datastream datastream = datastreamTask.getDatastream();
    String path =
        KeyBuilder.datastreamTaskStateKey(_cluster, datastream.getConnectorType(),
            datastream.getName(), datastreamTask.getId(), key);
    _zkclient.ensurePath(path);
    _zkclient.writeData(path, value);
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
      _zkclient.subscribeChildChanges(_path, this);
      _datastreams = _zkclient.getChildren(_path, true);
    }

    public void close() {
      _zkclient.unsubscribeChildChanges(_path, this);
    }

    @Override
    public synchronized void handleChildChange(String parentPath, List<String> currentChildren) throws Exception {
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
      _zkclient.subscribeChildChanges(_path, this);
      _liveInstances = getLiveInstanceNames(_zkclient.getChildren(_path));
      cleanUpDeadInstances();
    }

    private void cleanUpDeadInstances() {
      List<String> deadInstances = _zkclient.getChildren(KeyBuilder.instances(_cluster));
      deadInstances.removeAll(_liveInstances);
      for (String dead : deadInstances) {
        _zkclient.deleteRecursive(KeyBuilder.instance(_cluster, dead));
      }
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
      _zkclient.unsubscribeChildChanges(_path, this);
    }

    public List<String> getLiveInstances() {
      return _liveInstances;
    }

    @Override
    public void handleChildChange(String parentPath, List<String> currentChildren) throws Exception {
      List<String> liveInstances = getLiveInstanceNames(_zkclient.getChildren(_path));
      List<String> deadInstances = new ArrayList<>(_liveInstances);
      deadInstances.removeAll(liveInstances);
      for (String dead : deadInstances) {
        _zkclient.deleteRecursive(KeyBuilder.instance(_cluster, dead));
      }

      _liveInstances = liveInstances;

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
   * ZkBackedDatastreamTasksMap provides informations about all DatastreamTasks existing in the cluster
   * grouped by the connector type. That is, this map will obtain
   */
  public class ZkBackedDatastreamTasksMap implements IZkChildListener {
    private Map<String, List<DatastreamTask>> _allDatastreamTasks = null;
    private String _path = KeyBuilder.connectors(_cluster);

    public ZkBackedDatastreamTasksMap() {
      _zkclient.ensurePath(_path);
    }

    public Map<String, List<DatastreamTask>> getAllDatastreamTasks() {
      if (_allDatastreamTasks == null) {
        reloadData();
      }
      return _allDatastreamTasks;
    }

    private synchronized void reloadData() {
      _allDatastreamTasks = new HashMap<>();
      List<String> connectorTypes = _zkclient.getChildren(_path);

      for (String connectorType: connectorTypes) {
        if (!_allDatastreamTasks.containsKey(connectorType)) {
          _allDatastreamTasks.put(connectorType, new ArrayList<>());
        }

        String path = KeyBuilder.connector(_cluster, connectorType);
        List<String> tasksForConnector = _zkclient.getChildren(path);

        for (String taskName: tasksForConnector) {
          String p = KeyBuilder.connectorTask(_cluster, connectorType, taskName);
          // read the DatastreamTask json data
          String content = _zkclient.ensureReadData(p);
          // deserialize to DatastreamTask
          DatastreamTask t = DatastreamTaskImpl.fromJson(content);
          _allDatastreamTasks.get(connectorType).add(t);
        }
      }
    }

    @Override
    public void handleChildChange(String parentPath, List<String> currentChilds) throws Exception {
      reloadData();
    }
  }

  public class ZkBackedTaskListProvider implements IZkChildListener {
    private String _path = KeyBuilder.instanceAssignments(_cluster, _instanceName);

    public ZkBackedTaskListProvider() {
      _zkclient.subscribeChildChanges(_path, this);
    }

    public void close() {
      _zkclient.unsubscribeChildChanges(KeyBuilder.instanceAssignments(_cluster, _instanceName), this);
    }

    @Override
    public synchronized void handleChildChange(String parentPath, List<String> currentChildren) throws Exception {
      if (_listener != null) {
        _listener.onAssignmentChange();
      }
    }
  }

}
