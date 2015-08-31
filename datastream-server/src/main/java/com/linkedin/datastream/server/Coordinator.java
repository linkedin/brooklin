package com.linkedin.datastream.server;

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

import java.util.List;
import java.util.Map;
import java.util.HashMap;

import com.linkedin.datastream.server.zk.CoordinatorZkAdapter;
import org.apache.log4j.Logger;

public class Coordinator implements LeaderChangeListener, AssignmentChangeListener {
    private static final Logger LOG = Logger.getLogger(Coordinator.class.getName());

    private String _cluster;
    private CoordinatorZkAdapter _adapter;

    private Map<Connector, AssignmentStrategy> _connectors = new HashMap<>();

    public Coordinator(String zkServers, String cluster) {
        _cluster = cluster;
        _adapter = new CoordinatorZkAdapter(zkServers, cluster);
        _adapter.setLeaderChangeListener(this);
        _adapter.setAssignmentChangeListener(this);
    }

    public void start() {
        for(Connector connector : _connectors.keySet()) {
            connector.start();
        }
    }

    public void stop() {
        for(Connector connector : _connectors.keySet()) {
            connector.stop();
        }
    }

    private void doAssignment() {

    }

    @Override
    public void onBecomeLeader() {
        LOG.info("Coordinator instance " + _adapter.getInstanceName() + " becomes leader");
        doAssignment();
    }

    @Override
    public void onBecomeFollower() {
        LOG.info("Coordinator instance " + _adapter.getInstanceName() + " becomes follower");
    }

    @Override
    public void onAssignment(List<String> tasks) {

    }

    public void addConnector(Connector connector, AssignmentStrategy strategy) {
        _connectors.put(connector, strategy);
    }
}