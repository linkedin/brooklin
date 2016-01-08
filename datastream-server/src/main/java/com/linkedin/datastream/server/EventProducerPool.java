package com.linkedin.datastream.server;

import com.linkedin.datastream.common.VerifiableProperties;
import com.linkedin.datastream.server.api.schemaregistry.SchemaRegistryProvider;
import com.linkedin.datastream.server.api.transport.TransportProvider;
import com.linkedin.datastream.server.providers.CheckpointProvider;

import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.commons.lang.Validate;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * This class manages creation of EventProducers for tasks per connector.
 * The coordinator uses this to create producers before passing it on to the connectors
 */
public class EventProducerPool {
  private static final String CONFIG_PRODUCER = "datastream.eventProducer";

  // Map between Connector type and <Destination URI, Producer>
  private final Map<String, Map<String, EventProducer>> _producers = new HashMap<>();
  private final Map<DatastreamTask, DatastreamEventProducer> _taskEventProducerMap = new HashMap<>();
  private final SchemaRegistryProvider _schemaRegistryProvider;
  private final TransportProvider _transportProvider;

  private CheckpointProvider _checkpointProvider;
  private Properties _config;

  private static final Logger LOG = LoggerFactory.getLogger(EventProducerPool.class.getName());

  public EventProducerPool(CheckpointProvider checkpointProvider, TransportProvider transportProvider,
      SchemaRegistryProvider schemaRegistryProvider, Properties config) {

    Validate.notNull(checkpointProvider, "null checkpoint provider");
    Validate.notNull(transportProvider, "null transport provider");
    Validate.notNull(config, "null config");

    _checkpointProvider = checkpointProvider;
    _transportProvider = transportProvider;
    _schemaRegistryProvider = schemaRegistryProvider;
    _config = config;
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
      LOG.info("Tasks is empty");
      return new HashMap<>();
    }

    // Mapping between the task and the producer.This is the result that is returned
    Map<DatastreamTask, DatastreamEventProducer> taskProducerMapping = new HashMap<>();

    // List of already created producers for the specified connector type
    Map<String, EventProducer> producersForConnectorType = _producers.get(connectorType);

    if (producersForConnectorType == null) {
      producersForConnectorType = new HashMap<>();
      _producers.put(connectorType, producersForConnectorType);
    }

    // List of producers that don't have a corresponding task. These producers need to be shutdown
    Map<String, EventProducer> unusedProducerMap = new HashMap<>(producersForConnectorType);

    VerifiableProperties properties = new VerifiableProperties(_config);

    // Check if we can reuse existing EventProducers
    for (DatastreamTask task : tasks) {
      String destination = task.getDatastreamDestination().getConnectionString();
      if (producersForConnectorType.containsKey(destination)) {
        // TODO: Producer will implement a AddTask() and at that time we need to add the task to the producer
        // There is a producer for the specified destination.
        unusedProducerMap.remove(destination);
      } else {
        LOG.info(String.format("Creating new message producer for destination %s and task %s", destination, task));
        ArrayList<DatastreamTask> tasksPerProducer = new ArrayList<DatastreamTask>();
        tasksPerProducer.add(task);
        EventProducer
            eventProducer = new EventProducer(tasksPerProducer, _transportProvider,
            _checkpointProvider, properties.getDomainProperties(CONFIG_PRODUCER), customCheckpointing);
        producersForConnectorType.put(destination, eventProducer);
      }

      if(!_taskEventProducerMap.containsKey(task)) {
        _taskEventProducerMap.put(task, new DatastreamEventProducerImpl(task, _schemaRegistryProvider,
            producersForConnectorType.get(destination)));
      }

      taskProducerMapping.put(task, _taskEventProducerMap.get(task));
    }

    // Remove the unused producers from the producer pool.
    for (String destination : unusedProducerMap.keySet()) {
      producersForConnectorType.remove(destination);
    }

    unusedProducers.addAll(unusedProducerMap.values());

    return taskProducerMapping;
  }
}
