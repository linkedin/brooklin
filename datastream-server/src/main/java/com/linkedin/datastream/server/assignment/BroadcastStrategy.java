package com.linkedin.datastream.server.assignment;

/*
 *
 * Copyright 2015 LinkedIn Corp. All rights reserved
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
*/

import com.linkedin.datastream.common.Datastream;
import com.linkedin.datastream.server.AssignmentStrategy;
import com.linkedin.datastream.server.DatastreamTask;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BroadcastStrategy implements AssignmentStrategy {
    @Override
    public Map<String, List<DatastreamTask>> assign(List<Datastream> datastreams, List<String> instances, Map<String, List<DatastreamTask>> currentAssignment) {
        Map<String, List<DatastreamTask>> assignment = new HashMap<>();

        List<DatastreamTask> templateAssignment = new ArrayList<>();
        for(Datastream stream : datastreams) {
            templateAssignment.add(new DatastreamTask(stream));
        }

        for(String instance: instances) {
            // make a clone from the template so they are not sharing the same references
            List<DatastreamTask> clone = new ArrayList<>(templateAssignment);
            assignment.put(instance, clone);
        }

        return assignment;
    }
}