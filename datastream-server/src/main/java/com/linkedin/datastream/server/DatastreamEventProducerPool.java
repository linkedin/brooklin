package com.linkedin.datastream.server;

import com.linkedin.datastream.server.providers.CheckpointProvider;
import org.apache.commons.lang.Validate;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class DatastreamEventProducerPool {
  private final CheckpointProvider _checkpointProvider;
  private final TransportProviderFactory _transportProviderFactory;
  private final Properties _globalConfig;

  public DatastreamEventProducerPool(CheckpointProvider checkpointProvider,
                                     TransportProviderFactory transportProviderFactory,
                                     Properties globalConfig) {
    Validate.notNull(checkpointProvider, "null checkpoint provider");
    Validate.notNull(transportProviderFactory, "null transport provider factory");
    Validate.notNull(globalConfig, "null config");

    _checkpointProvider = checkpointProvider;
    _transportProviderFactory = transportProviderFactory;
    _globalConfig = globalConfig;
  }

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
