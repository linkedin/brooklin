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

import java.io.IOException;
import java.util.List;

import com.linkedin.datastream.common.Datastream;
import com.linkedin.datastream.server.assignment.BroadcastStrategy;
import com.linkedin.datastream.testutil.EmbeddedZookeeper;
import org.apache.log4j.Logger;

import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class TestCoordinator {
    private static final Logger LOG = Logger.getLogger(TestCoordinator.class.getName());

    EmbeddedZookeeper _embeddedZookeeper;
    String _zkConnectionString;

    @BeforeMethod
    public void setup() throws IOException {
        _embeddedZookeeper = new EmbeddedZookeeper();
        _zkConnectionString = _embeddedZookeeper.getConnection();
        _embeddedZookeeper.startup();
    }

    @AfterMethod
    public void teardown() throws IOException {
        _embeddedZookeeper.shutdown();
    }

    public static class TestLoggingConnector implements Connector {
        boolean _isStarted = false;

        public boolean isStarted() {
            return _isStarted;
        }

        public boolean isStopped() {
            return !_isStarted;
        }

        @Override
        public void start() {
            _isStarted = true;
        }

        @Override
        public void stop() {
            _isStarted = false;
        }

        @Override
        public void onAssignmentChange(DatastreamContext context, List<DatastreamTask> tasks) {

        }

        @Override
        public DatastreamTarget getDatastreamTarget(Datastream stream) {
            return null;
        }

        @Override
        public String getConnectorType() {
            return "TestConnector";
        }
    }

    @Test
    public void testConnectorStartStop() throws Exception {
        String testCluster = "test_coordinator_startstop";
        Coordinator coordinator = new Coordinator(_zkConnectionString, testCluster);

        TestLoggingConnector connector = new TestLoggingConnector();

        coordinator.addConnector(connector, new BroadcastStrategy());

        coordinator.start();
        Assert.assertTrue(connector.isStarted());

        coordinator.stop();
        Assert.assertTrue(connector.isStopped());
    }
}