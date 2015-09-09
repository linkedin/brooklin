package com.linkedin.datastream.server;

import java.util.List;
import java.util.Map;

import com.linkedin.datastream.common.Datastream;

public interface AssignmentStrategy {
    /**
     * assign a list of datastreams to a list of instances, and return a map from instances-> list of streams.
     *
     * @param datastreams all data streams defined in Datastream Management Service to be assigned
     * @param instances all live instances
     * @param currentAssignment existing
     * @return
     */
    Map<String, List<DatastreamTask>> assign(List<Datastream> datastreams,
                                                    List<String> instances, 
                                                    Map<String, List<DatastreamTask>> currentAssignment);
}