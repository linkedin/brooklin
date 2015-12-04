package com.linkedin.datastream.server;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class EventProducerPool {

  public synchronized Map<DatastreamTask, EventProducer> getEventProducers(List<DatastreamTask> tasks) {
    Map<DatastreamTask, EventProducer> producerMap = new HashMap<>();
    for(DatastreamTask task : tasks) {
      producerMap.put(task, new EventProducerImpl());
    }

    return producerMap;
  }
}
