package com.linkedin.datastream.server;

import com.linkedin.datastream.common.Datastream;
import java.util.List;

public interface Connector {
    /**
     * Method to start the connector. This is called immediately after the connector is instantiated.
     * This typically happens when datastream instance starts up.
     */
    void start();

    /**
     * Method to stop the connector. This is called when the datastream instance is being stopped.
     */
    void stop();

    String getConnectorType();

    /**
     * callback when the datastreams assignment to this instance is changed. This is called whenver
     * there is a change for the assignment. The implementation of the Connector is responsible
     * to keep a state of the previous assignment.
     *
     * @param context context information including producer
     * @param tasks the list of the current assignment.
     */
    void onAssignmentChange(DatastreamContext context, List<DatastreamTask> tasks);

    /**
     * Provides a DatastreamTarget object with information of downstream
     * Kafka topic to which the connector will be producing change events.
     *
     * @param stream: Datastream model
     * @return populated DatastreamTarget
     */
    DatastreamTarget getDatastreamTarget(Datastream stream);
}
