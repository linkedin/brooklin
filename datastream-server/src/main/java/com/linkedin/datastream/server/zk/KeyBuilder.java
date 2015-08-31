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

public class KeyBuilder {
    private static final String cluster ="/%s";
    private static final String _liveInstances = "/%s/instances";
    private static final String liveInstancePath = "/%s/instances/%s";

    static String cluster(String clusterName) {
        return String.format(cluster, clusterName);
    }

    static String instances(String cluster) {
        return String.format(_liveInstances, cluster);
    }

    static String instance(String cluster, String instanceName) {
        return String.format(liveInstancePath, cluster, instanceName);
    }
}