package com.linkedin.datastream.server.zk;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Random;
import java.util.Collections;
import java.util.List;

import com.linkedin.datastream.server.AssignmentChangeListener;
import com.linkedin.datastream.server.LeaderChangeListener;
import org.I0Itec.zkclient.IZkChildListener;
import org.apache.log4j.Logger;

import org.I0Itec.zkclient.IZkDataListener;
import org.apache.zookeeper.CreateMode;


public class CoordinatorZkAdapter {
    String _zkServers;
    String _cluster;
    int _sessionTimeout;
    int _connectionTimeout;

    ZkClient _zkclient;
    String _instanceName;

    private Boolean _isLeader = null;
    private LeaderChangeListener _leaderChangeListener;
    private AssignmentChangeListener _assignmentChangeListener;

    // the current znode this node is listening to
    private String _currentSubscription = null;

    private Random randomGenerator = new Random();

    private static final Logger LOG = Logger.getLogger(CoordinatorZkAdapter.class.getName());

    public CoordinatorZkAdapter(String zkServers, String cluster) {
        this(zkServers, cluster, ZkClient.DEFAULT_SESSION_TIMEOUT, ZkClient.DEFAULT_CONNECTION_TIMEOUT);
    }

    public CoordinatorZkAdapter(String zkServers, String cluster, int sessionTimeout, int connectionTimeout) {
        _zkServers = zkServers;
        _cluster = cluster;
        _sessionTimeout = sessionTimeout;
        _connectionTimeout = connectionTimeout;

        connect();
    }

    public void setLeaderChangeListener(LeaderChangeListener listener) {
        _leaderChangeListener = listener;
    }
    public void setAssignmentChangeListener(AssignmentChangeListener listener) {
        _assignmentChangeListener = listener;
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

    // watch the instances node in zookeeper to trigger leader election
    private ZkLeaderElectionListener leaderElectionListener = new ZkLeaderElectionListener();

    // watch the live instance tree in zookeeper to trigger assignment change actions.
    private ZkInstanceAssignmentChangeListener assignmentChangeListener = new ZkInstanceAssignmentChangeListener();

    public void connect() {
        _zkclient = new ZkClient(_zkServers, _sessionTimeout, _connectionTimeout);

        // create a globally uniq instance name and create a live instance node in zookeeper
        _instanceName = generateInstanceName();

        // create a ephemeral live instance node for this coordinator
        LOG.info("creating live instance node for coordinator with name: " + _instanceName);

        // make sure the live instance path exists
        _zkclient.ensurePath(KeyBuilder.instances(_cluster));
        _zkclient.create(KeyBuilder.instance(_cluster, _instanceName), "", CreateMode.EPHEMERAL);

        // join leader election
        joinLeaderElection();

        // join as a live instance
        joinLiveInstance();
    }

    // each instance of coordinator also watch its own instance tree for assignment changes
    private void joinLiveInstance() {
        // get the path to the instance node
        String instanceZNode = KeyBuilder.instance(_cluster, _instanceName);
        _zkclient.subscribeChildChanges(instanceZNode, assignmentChangeListener);
    }

    // each instance of coordinator (and coordinator zk adapter) must participate the leader
    // election. This method will be called when the zk connection is made. It can also be
    // called recursively in corner cases.
    private void joinLeaderElection() {
        // get the list of current live instances
        String electionPath = KeyBuilder.instances(_cluster);
        List<String> liveInstances = _zkclient.getChildren(electionPath);
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

        // if this instance is the leader
        if (index == 0) {
            if (_isLeader == null ||  _isLeader == false) {
                _isLeader = true;
                if (_leaderChangeListener != null) {
                    _leaderChangeListener.onBecomeLeader();
                }
            }
            return;
        }

        // if this instance is not the leader
        if (_isLeader == null || _isLeader == true) {
            _isLeader = false;
            if (_leaderChangeListener != null) {
                _leaderChangeListener.onBecomeFollower();
            }
        }

        // prevCandidate is the leader candidate that is in line before this current node
        // we only become the leader after prevCandidate goes offline
        String prevCandidate = liveInstances.get(index-1);

        // if the prev candidate is not the current subscription, reset it
        if (prevCandidate != _currentSubscription) {
            if (_currentSubscription != null) {
                _zkclient.unsubscribeDataChanges(_currentSubscription, leaderElectionListener);
            }

            _currentSubscription = prevCandidate;
            _zkclient.subscribeDataChanges(KeyBuilder.instance(_cluster, _currentSubscription), leaderElectionListener);
        }

        // now double check if the previous candidate exists. If not, try election again
        boolean exists = _zkclient.exists(KeyBuilder.instance(_cluster, prevCandidate), true);

        if (exists) {
            if (_isLeader == null || _isLeader == true) {
                _isLeader = false;
                if (_leaderChangeListener != null) {
                    _leaderChangeListener.onBecomeFollower();
                }
            }
        } else {
            try {
                Thread.sleep(randomGenerator.nextInt(1000));
            } catch (InterruptedException ie) {
            }
            joinLeaderElection();
        }
    }

    // handle assignment change. This method is called when the children of the
    // live instance node are changed.
    private void handleAssignmentChange(List<String> assignments) {

    }

    private String generateInstanceName() {
        assert _zkclient != null;

        // random hostname
        String hostname = "coordinator" + randomGenerator.nextInt(1000);

        try {
            hostname = InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException uhe) {
            LOG.error(uhe);
        }

        long timestamp = System.currentTimeMillis();
        String instanceName = String.format("%s-%d", hostname, timestamp);

        // in very rare case when there is a name conflict, retry
        String znodePath = KeyBuilder.instance(_cluster, instanceName);
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

    public class ZkInstanceAssignmentChangeListener implements IZkChildListener {
        @Override
        public void handleChildChange(String parentPath, List<String> currentAssignments) throws Exception {
            _assignmentChangeListener.onAssignment(currentAssignments);
        }
    }

}