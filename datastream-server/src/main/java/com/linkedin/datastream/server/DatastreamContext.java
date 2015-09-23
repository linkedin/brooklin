package com.linkedin.datastream.server;

import com.linkedin.datastream.server.DatastreamTask;

public interface DatastreamContext {
    // obtain the last known persisted DatastreamState. The Connector
    // implementation can use this method to obtain the last know
    // checkpoint.
    String getState(DatastreamTask datastream, String key);

    // persiste the datastreamstate. The Connector implementation can
    // use this method to persist the last known checkpoint in zookeeper
    void saveState(DatastreamTask datastream, String key, String value);
}