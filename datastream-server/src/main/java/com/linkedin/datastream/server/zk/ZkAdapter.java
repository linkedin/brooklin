package com.linkedin.datastream.server.zk;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;

import com.linkedin.datastream.common.Datastream;
import com.linkedin.datastream.common.DatastreamJSonUtil;
import com.linkedin.datastream.server.DatastreamTask;
import org.I0Itec.zkclient.IZkChildListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.I0Itec.zkclient.IZkDataListener;
import org.apache.zookeeper.CreateMode;

/**
 *
 *
 *
 *        ZooKeeper
 * ┌─────────────────────┐          ┌───────────────────────────────────────────────────┐
 * │                     │          │                     ZkAdapter                     │
 * │- cluster            │          │                                                   │
 * │  |- instances       │          │┌───────────────────────────────────┐    ┌─────────┤
 * │  |  |- i001         │┌─────────┼▶  ZkBackedTaskListProvider         │    │         │   ┌────────────────┐
 * │  |  |- i002         ││         │└───────────────────────────────────┘    │         │───▶ onBecomeLeader │
 * │  |         ─────────┼┘         │┌───────────────────────────────────┐    │         │   └────────────────┘
 * │  |- liveinstances ──┼──────────▶│ ZkBackedLiveInstanceListProvider  │    │         │   ┌──────────────────┐
 * │  |  |- i001─────────┼┐         │└───────────────────────────────────┘    │         │───▶ onBecomeFollower │
 * │  |  |- i002         ││         │┌───────────────────────────────────┐    │ZkAdapter│   └──────────────────┘
 * │  |                  │└─────────▶│     ZkLeaderElectionListener      │    │Listener │   ┌────────────────────┐
 * │  |- Espresso────────┼─┐        │└───────────────────────────────────┘    │         │───▶ onAssignmentChange │
 * │  |- Oracle          │ │        │┌───────────────────────────────────┐    │         │   └────────────────────┘
 * │                     │ └────────┼▶    ZkBackedDatastreamTasksMap     │    │         │   ┌───────────────────────┐
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

    private List<String> _instances;

    public ZkAdapter(String zkServers, String cluster) {
        this(zkServers, cluster, ZkClient.DEFAULT_SESSION_TIMEOUT, ZkClient.DEFAULT_CONNECTION_TIMEOUT, null);
    }

    public ZkAdapter(String zkServers, String cluster, ZkAdapterListener listener) {
        this(zkServers, cluster, ZkClient.DEFAULT_SESSION_TIMEOUT, ZkClient.DEFAULT_CONNECTION_TIMEOUT, listener);
    }

    public ZkAdapter(String zkServers, String cluster, int sessionTimeout, int connectionTimeout) {
        this(zkServers, cluster, ZkClient.DEFAULT_SESSION_TIMEOUT, ZkClient.DEFAULT_CONNECTION_TIMEOUT, null);
    }

    public ZkAdapter(String zkServers, String cluster, int sessionTimeout, int connectionTimeout, ZkAdapterListener listener) {
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

    public void disconnect() {
        _zkclient.close();
        _zkclient = null;
    }

    /**
     * Connect the adapter so that it can connect and bridge events between zookeeper changes and
     * the actions that needs to be taken with them, which are implemented in the Coordinator class
     *
     */
    public void connect() {
        _zkclient = new ZkClient(_zkServers, _sessionTimeout, _connectionTimeout);

        // create a globally uniq instance name and create a live instance node in zookeeper
        _instanceName = generateInstanceName();

        // create a ephemeral live instance node for this coordinator
        LOG.info("creating live instance node for coordinator with name: " + _instanceName);

        // make sure the live instance path exists
        _zkclient.ensurePath(KeyBuilder.instance(_cluster, _instanceName));
        _zkclient.ensurePath(KeyBuilder.liveInstances(_cluster));
        _zkclient.create(KeyBuilder.liveInstance(_cluster, _instanceName), "", CreateMode.EPHEMERAL);

        // start with follower state, then join leader election
        onBecomeFollower();
        joinLeaderElection();

        // both leader and follower needs to listen to its own instance change
        _assignmentList = new ZkBackedTaskListProvider();
        // each instance will need the full map of all datastream tasks
        _datastreamMap = new ZkBackedDatastreamTasksMap();


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
     *  each instance of coordinator (and coordinator zk adapter) must participate the leader
     *  election. This method will be called when the zk connection is made. It can also be
     *  called recursively in corner cases.
     */
    private void joinLeaderElection() {

        // get the list of current live instances
        List<String> liveInstances = _zkclient.getChildren(KeyBuilder.liveInstances(_cluster));
        Collections.sort(liveInstances);

        // find the position of the current instance in the list
        String[] nodePathParts = _instanceName.split("/");
        String nodeName = nodePathParts[nodePathParts.length - 1];
        int index = liveInstances.indexOf(nodeName);

        if (index < 0) {
            // only when the zookeeper session already expired by the time this adapter joins for leader election.
            // mostly because the zkclient session expiration timeout.
            LOG.error("Failed to join leader election. Try reconnect the zookeeper");
            connect();
        }

        // if this instance is first in line to become leader. Check if it is already a leader.
        // if so, do nothing. Otherwise, set the _isLeader status and trigger onBecomeLeader() callback.
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
        String prevCandidate = liveInstances.get(index-1);

        // if the prev candidate is not the current subscription, reset it
        if (prevCandidate != _currentSubscription) {
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

        for(String streamNode: datastreamNames) {
            String path = KeyBuilder.datastream(_cluster, streamNode);
            String content = _zkclient.readData(path);
            Datastream stream = DatastreamJSonUtil.getDatastreamFromJsonString(content);
            result.add(stream);
        }

        return result;
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
        String path = KeyBuilder.instance(_cluster, instance);
        return _zkclient.getChildren(path);
    }

    /**
     * given an instance name and a datastreamtask name assigned to this instance, return the DatastreamTask
     *
     * @param instance
     * @param taskName
     * @return
     */
    public DatastreamTask getAssignedDatastreamTask(String instance, String taskName) {
        String content = _zkclient.readData(KeyBuilder.datastreamTask(_cluster, instance, taskName));
        DatastreamTask task = DatastreamTask.fromJson(content);
        String dsName = task.getDatastreamName();

        String dsContent = _zkclient.readData(KeyBuilder.datastream(_cluster, dsName));
        Datastream stream = DatastreamJSonUtil.getDatastreamFromJsonString(dsContent);
        task.setDatastream(stream);
        return task;
    }

    /**
     * update the task assignment of a given instance. This method is only called by the
     * coordinator leader. To execute the update, first retrieve the existing assignment,
     * then capture the difference, and only act on the differences. That is, add new
     * assignments, remove old assignments. For ones that didn't change, do nothing.
     *
     * @param instance
     * @param assignments
     */
    public void updateInstanceAssignment(String instance, List<DatastreamTask> assignments) {
        List<String> oldAssignmentNodes = _zkclient.getChildren(KeyBuilder.instance(_cluster, instance));
        List<DatastreamTask> oldAssignment = new ArrayList<>();

        //
        // retrieve the existing list of DatastreamTask assignment for the current instance
        //
        oldAssignmentNodes.forEach(n -> {
            String path = KeyBuilder.datastreamTask(_cluster, instance, n);
            String content = _zkclient.readData(path);
            DatastreamTask task = DatastreamTask.fromJson(content);
            oldAssignment.add(task);
        });

        //
        // find the DatastreamTasks that are to be deleted from this instance, delete the znodes from zookeeper
        //
        List<DatastreamTask> removed = new ArrayList<>(oldAssignment);
        removed.removeAll(assignments);
        removed.forEach(ds -> {
            String path = KeyBuilder.datastreamTask(_cluster, instance, ds.getName());
            LOG.info("update assignment: remove task " + ds.getName() + " from instance " + instance);
            _zkclient.delete(path);
        });

        //
        // find newly added DatastreamTasks and create corresponding znodes
        //
        List<DatastreamTask> added = new ArrayList<>(assignments);
        added.removeAll(oldAssignment);
        added.forEach(ds -> {
            String path = KeyBuilder.datastreamTask(_cluster, instance, ds.getName());
            LOG.info("update assignment: add task " + ds.getName() + " for instance " + instance);
            try {
                _zkclient.create(path, ds.toJson(), CreateMode.PERSISTENT);
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
    }

    private String generateInstanceName() {
        assert _zkclient != null;

        // random hostname
        String hostname = "coordinator" + randomGenerator.nextInt(1000);

        try {
            hostname = InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException uhe) {
            LOG.error(uhe.getMessage());
        }

        long timestamp = System.currentTimeMillis();
        String instanceName = String.format("%s-%d", hostname, timestamp);

        // in very rare case when there is a name conflict, retry
        String znodePath = KeyBuilder.liveInstance(_cluster, instanceName);
        if (_zkclient.exists(znodePath)) {
            try {
                int randomInt = randomGenerator.nextInt(20);
                Thread.sleep(randomInt);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            return generateInstanceName();
        }

        return instanceName;
    }

    /**
     * ZkAdapterListener
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
        public void handleChildChange(String parentPath, List<String> currentChildren) throws Exception {
            _datastreams = _zkclient.getChildren(_path);
            _listener.onDatastreamChange();
        }

        public List<String> getDatastreamNames() {
            return _datastreams;
        }
    }

    public class ZkBackedLiveInstanceListProvider implements IZkChildListener {
        private List<String> _liveInstances = new ArrayList<>();
        private String _path;

        public ZkBackedLiveInstanceListProvider() {
            _path = KeyBuilder.liveInstances(_cluster);
            _zkclient.ensurePath(_path);
            _zkclient.subscribeChildChanges(_path, this);
            _liveInstances = _zkclient.getChildren(_path);
        }

        public void close() {
            _zkclient.unsubscribeChildChanges(_path, this);
        }

        public List<String> getLiveInstances() {
            return _liveInstances;
        }

        @Override
        public void handleChildChange(String parentPath, List<String> currentChildren) throws Exception {
            _liveInstances = _zkclient.getChildren(_path);

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

    public class ZkBackedDatastreamTasksMap implements IZkChildListener {
        private Map<String, List<DatastreamTask>> _allDatastreamTasks = null;
        private String _path = KeyBuilder.cluster(_cluster);

        public ZkBackedDatastreamTasksMap() {
            _zkclient.ensurePath(_path);
            reloadData();
        }

        public Map<String, List<DatastreamTask>> getAllDatastreamTasks() {
            return _allDatastreamTasks;
        }

        private void reloadData() {
            _allDatastreamTasks = new HashMap<>();
            List<String> connectorTypes = _zkclient.getChildren(_path);
            connectorTypes.removeAll(Arrays.asList("datastream", "instances", "liveinstances"));

            connectorTypes.forEach(connectorType -> {
                if (!_allDatastreamTasks.containsKey(connectorType)) {
                    _allDatastreamTasks.put(connectorType, new ArrayList<>());
                }

                String path = KeyBuilder.connector(_cluster, connectorType);
                List<String> tasksForConnector = _zkclient.getChildren(path);

                tasksForConnector.forEach(taskName -> {
                    String p = KeyBuilder.connectorTask(_cluster, connectorType, taskName);
                    // read the DatastreamTask json data
                    String content = _zkclient.readData(p);
                    // deserialize to DatastreamTask
                    DatastreamTask t = DatastreamTask.fromJson(content);
                    _allDatastreamTasks.get(connectorType).add(t);
                });
            });
        }

        @Override
        public void handleChildChange(String parentPath, List<String> currentChilds) throws Exception {
            reloadData();
        }
    }

    public class ZkBackedTaskListProvider implements IZkChildListener {
        private List<String> _assigned = new ArrayList<>();
        private String _path = KeyBuilder.instance(_cluster, _instanceName);

        public ZkBackedTaskListProvider() {
            _assigned = _zkclient.getChildren(_path, true);
            _zkclient.ensurePath(_path);
            _zkclient.subscribeChildChanges(_path, this);
        }

        public List<String> getAssigned() {
            return _assigned;
        }

        public void close() {
            _zkclient.unsubscribeChildChanges(KeyBuilder.instance(_cluster, _instanceName), this);
        }

        @Override
        public void handleChildChange(String parentPath, List<String> currentChildren) throws Exception {
            _assigned = _zkclient.getChildren(_path);
            if (_listener != null) {
                _listener.onAssignmentChange();
            }
        }
    }

}