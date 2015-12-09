package com.linkedin.datastream.server;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DatastreamEventProducerPool {

  public synchronized Map<DatastreamTask, DatastreamEventProducer> getEventProducers(List<DatastreamTask> tasks) {
    Map<DatastreamTask, DatastreamEventProducer> producerMap = new HashMap<>();
    for(DatastreamTask task : tasks) {
      // TODO: returning dummy producer for now because real producer
      // requires actual tasks, transportProvider, and checkpointProvider
      producerMap.put(task, new DatastreamEventProducer() {
        @Override
        public void send(DatastreamEventRecord event) {

        }

        @Override
        public Map<DatastreamTask, String> getSafeCheckpoints() {
          return null;
        }

        @Override
        public void shutdown() {

        }
      });
    }

    return producerMap;
  }
}
