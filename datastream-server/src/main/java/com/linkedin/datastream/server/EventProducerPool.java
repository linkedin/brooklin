package com.linkedin.datastream.server;

import com.linkedin.datastream.common.DatastreamException;
import com.linkedin.datastream.common.VerifiableProperties;
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
  private final Map<String, Map<String, DatastreamEventProducer>> _producers = new HashMap<String, Map<String, DatastreamEventProducer>>();

  private CheckpointProvider _checkpointProvider;
  private TransportProviderFactory _transportProviderFactory;
  private Properties _config;

  private static final Logger LOG = LoggerFactory.getLogger(EventProducerPool.class.getName());

  public EventProducerPool(CheckpointProvider checkpointProvider, TransportProviderFactory transportProviderFactory,
      Properties config) {

    Validate.notNull(checkpointProvider, "null checkpoint provider");
    Validate.notNull(transportProviderFactory, "null transport provider factory");
    Validate.notNull(config, "null config");

    _checkpointProvider = checkpointProvider;
    _transportProviderFactory = transportProviderFactory;
    _config = config;
  }

  /**
  *
  * This method is called when the coordinator is assigned new datastream tasks
  * and is used to retrieve DatastreamEventProducer corresponding to the assigned tasks
   * @param tasks list of datastream tasks
   * @param connectorType type of connector.
  * @return map of task to event producer mapping for this connector type
  */
  public synchronized Map<DatastreamTask, DatastreamEventProducer> getEventProducers(List<DatastreamTask> tasks,
      String connectorType) {

    Validate.notNull(tasks);
    Validate.notNull(connectorType);
    Validate.notEmpty(connectorType);

    if (tasks.isEmpty()) {
      LOG.info("Tasks is empty");
      return new HashMap<>();
    }

    // Mapping between the task and the producer.This is the result that is returned
    Map<DatastreamTask, DatastreamEventProducer> taskProducerMapping = new HashMap<DatastreamTask, DatastreamEventProducer>();

    // List of already created producers for the specified connector type
    Map<String, DatastreamEventProducer> producersForConnectorType = _producers.get(connectorType);

    if (producersForConnectorType == null) {
      producersForConnectorType = new HashMap<>();
      _producers.put(connectorType, producersForConnectorType);
    }

    // List of producers that don't have a corresponding task. These producers need to be shutdown
    Map<String, DatastreamEventProducer> unusedProducers = new HashMap<String, DatastreamEventProducer>(producersForConnectorType);

    VerifiableProperties properties = new VerifiableProperties(_config);

    // Check if we can reuse existing EventProducers
    for (DatastreamTask task : tasks) {
      String destination = task.getDatastreamDestination().getConnectionString();
      if (producersForConnectorType.containsKey(destination)) {
        // TODO: Producer will implement a AddTask() and at that time we need to add the task to the producer
        // There is a producer for the specified destination.
        unusedProducers.remove(destination);
      } else {
        LOG.info(String.format("Creating new message producer for destination %s and task %s", destination, task));
        ArrayList<DatastreamTask> tasksPerProducer = new ArrayList<DatastreamTask>();
        tasksPerProducer.add(task);
        producersForConnectorType.put(destination,
            new DatastreamEventProducerImpl(tasksPerProducer,_transportProviderFactory.createTransportProvider(_config),
                _checkpointProvider, properties.getDomainProperties(CONFIG_PRODUCER)));
      }
      taskProducerMapping.put(task, producersForConnectorType.get(destination));
    }

    // Remove the unused producers from the producer pool.
    for (String destination : unusedProducers.keySet()) {
      producersForConnectorType.remove(destination);
    }
    // TODO: Call producer shutdown once event producer interface is ready
    unusedProducers.clear();

    return taskProducerMapping;
  }
}
