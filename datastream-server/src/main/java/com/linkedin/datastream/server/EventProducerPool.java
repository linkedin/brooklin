package com.linkedin.datastream.server;

import com.linkedin.datastream.server.api.schemaregistry.SchemaRegistryProvider;
import com.linkedin.datastream.server.api.transport.TransportProvider;
import com.linkedin.datastream.server.api.transport.TransportProviderFactory;
import com.linkedin.datastream.server.providers.CheckpointProvider;

import java.util.HashSet;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.commons.lang.Validate;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;


/**
 * This class manages creation of EventProducers for tasks per connector.
 * The coordinator uses this to create producers before passing it on to the connectors
 */
public class EventProducerPool {
  private static final Logger LOG = LoggerFactory.getLogger(EventProducerPool.class);

  // Map between Connector type and <Destination URI, Producer>
  private final Map<String, Map<String, EventProducer>> _producers = new HashMap<>();

  private final CheckpointProvider _checkpointProvider;
  private final SchemaRegistryProvider _schemaRegistryProvider;
  private final TransportProviderFactory _transportProviderFactory;
  private final Properties _transportProviderConfig;
  private final Properties _eventProducerConfig;

  public EventProducerPool(CheckpointProvider checkpointProvider,
                           SchemaRegistryProvider schemaRegistryProvider,
                           TransportProviderFactory transportProviderFactory,
                           Properties transportProviderConfig,
                           Properties eventProducerConfig) {

    Validate.notNull(checkpointProvider, "null checkpoint provider");
    Validate.notNull(schemaRegistryProvider, "null schema registry provider");
    Validate.notNull(transportProviderFactory, "null transport provider factory");
    Validate.notNull(transportProviderConfig, "null transport provider config");
    Validate.notNull(eventProducerConfig, "null event producer config");

    _checkpointProvider = checkpointProvider;
    _schemaRegistryProvider = schemaRegistryProvider;
    _transportProviderFactory = transportProviderFactory;
    _transportProviderConfig = transportProviderConfig;
    _eventProducerConfig = eventProducerConfig;
  }

  /**
   * This method is called when the coordinator is assigned new datastream tasks
   * and is used to retrieve DatastreamEventProducer corresponding to the assigned tasks
   * @param tasks list of datastream tasks
   * @param connectorType type of connector.
   * @param customCheckpointing  decides whether custom checkpointing needs to be used or datastream server provided
   *                             checkpointing.
   * @param unusedProducers list to hold the producers that no tasks are referencing anymore. Coordinator should call
   *                        shutdown on these producers once onAssignmentChange has been called on the owner connector.
   *
   * @return map of task to event producer mapping for this connector type
   */
  public synchronized Map<DatastreamTask, DatastreamEventProducer> getEventProducers(List<DatastreamTask> tasks,
      String connectorType, boolean customCheckpointing, List<EventProducer> unusedProducers) {

    Validate.notNull(tasks);
    Validate.notNull(connectorType);
    Validate.notEmpty(connectorType);
    Validate.notNull(unusedProducers);

    if (tasks.isEmpty()) {
      LOG.info("Task list is empty");
      return new HashMap<>();
    }

    // Mapping between the task and the producer.This is the result that is returned
    Map<DatastreamTask, DatastreamEventProducer> taskProducerMapping = new HashMap<>();

    // List of already created producers for the specified connector type
    Map<String, EventProducer> producerMap = _producers.get(connectorType);

    if (producerMap == null) {
      producerMap = new HashMap<>();
      _producers.put(connectorType, producerMap);
    }

    Map<String, List<DatastreamTask>> taskMap = new HashMap<>();

    for (DatastreamTask task : tasks) {
      String destination = task.getDatastreamDestination().getConnectionString();
      if (!taskMap.containsKey(destination)) {
        taskMap.put(destination, new ArrayList<>());
      }
      taskMap.get(destination).add(task);
    }

    Set<String> usedDestinations = new HashSet<>();
    for (String destination : taskMap.keySet()) {
      EventProducer eventProducer;
      if (producerMap.containsKey(destination)) {
        LOG.info("Reusing event producer for destination" + destination);
        eventProducer = producerMap.get(destination);
        eventProducer.updateTasks(taskMap.get(destination));

      } else {
        List<DatastreamTask> taskList = taskMap.get(destination);
        LOG.info(String.format("Creating event producer for destination %s and task %s", destination, taskList));
        // Each distinct destination has its own transport provider
        TransportProvider transport = _transportProviderFactory.createTransportProvider(_transportProviderConfig);
        eventProducer = new EventProducer(taskList, transport, _checkpointProvider, _eventProducerConfig,
                customCheckpointing);
        producerMap.put(destination, eventProducer);
      }

      // No need to reuse DatastreamEventProducer which is just a thin wrapper of EventProducer
      taskMap.get(destination).forEach(t ->
        taskProducerMapping.put(t, new DatastreamEventProducerImpl(t, _schemaRegistryProvider, eventProducer))
      );

      usedDestinations.add(destination);
    }

    // Collect deprecated event producers and remove their destinations
    Set<String> unusedDestinations = producerMap.keySet().stream().filter(
            d -> !usedDestinations.contains(d)).collect(Collectors.toSet());

    unusedProducers.clear();
    for (String destination : unusedDestinations) {
      unusedProducers.add(producerMap.get(destination));
      producerMap.remove(destination);
    }

    return taskProducerMapping;
  }

  /**
   * Shutdown all outstanding event producers. This should only be called by Coordinator.shutdown()
   */
  public synchronized void shutdown() {
    if (_producers.size() == 0) {
      return;
    }

    LOG.info("Shutting down all producers in event producer pool");
    for (Map<String, EventProducer> producerMap : _producers.values()) {
      producerMap.values().forEach(producer -> producer.shutdown());
    }

    _producers.clear();
  }
}
