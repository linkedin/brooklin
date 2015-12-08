package com.linkedin.datastream.server;

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

  // Map between Connector type and <Destination URI, Producer>
  private final Map<String, Map<String, EventProducer>> _producers = new HashMap<String, Map<String, EventProducer>>();

  private static final Logger LOG = LoggerFactory.getLogger(EventProducerPool.class.getName());

  /**
  *
  * This method is called when the coordinator is assigned new datastream tasks
  * and is used to retrieve EventProducer corresponding to the assigned tasks
   * @param tasks list of datastream tasks
   * @param connectorType type of connector.
  * @return map of task to event producer mapping for this connector type
  */
  public synchronized Map<DatastreamTask, EventProducer> getEventProducers(List<DatastreamTask> tasks,
      String connectorType) {

    Validate.notNull(tasks);
    Validate.notNull(connectorType);
    Validate.notEmpty(connectorType);

    if (tasks.isEmpty()) {
      LOG.info("Tasks is empty");
      return new HashMap<DatastreamTask, EventProducer>();
    }

    // Mapping between the task and the producer.
    Map<DatastreamTask, EventProducer> taskProducerMapping = new HashMap<DatastreamTask, EventProducer>();

    // Tasks for which there is no producer in the pool
    List<DatastreamTask> unmappedTasks = new ArrayList<DatastreamTask>();

    // List of already created producers for the specified connector type
    Map<String, EventProducer> existingProducersForConnectorType = _producers.get(connectorType);

    if (existingProducersForConnectorType == null) {
      existingProducersForConnectorType = new HashMap<String, EventProducer>();
      _producers.put(connectorType, existingProducersForConnectorType);
    }

    // List of producers that don't have a corresponding task
    Map<String, EventProducer> unusedProducers = new HashMap<String, EventProducer>(existingProducersForConnectorType);

    // Check if we can reuse existing EventProducers
    for (DatastreamTask task : tasks) {
      String destination = task.getDatastreamDestination().getConnectionString();
      if (existingProducersForConnectorType != null && existingProducersForConnectorType.get(destination) != null) {
        taskProducerMapping.put(task, existingProducersForConnectorType.get(destination));
        unusedProducers.remove(destination);
      } else {
        unmappedTasks.add(task);
      }
    }

    // Remove the unused producers from the producer pool.
    for (String destination : unusedProducers.keySet()) {
      existingProducersForConnectorType.remove(destination);
    }
    // TODO: Call producer shutdown once event producer interface is ready
    unusedProducers.clear();

    if (unmappedTasks.size() == 0) {
      // all tasks are mapped to a producer.
      return taskProducerMapping;
    }

    // Create new producers for unmapped tasks
    for (DatastreamTask task : unmappedTasks) {
      String destination = task.getDatastreamDestination().getConnectionString();

      EventProducer producer = existingProducersForConnectorType.get(destination);
      if (producer == null) {
        LOG.info(String.format("Creating new message producer for destination %s and task %s", destination, task));
        producer = new EventProducerImpl();
        existingProducersForConnectorType.put(destination, producer);
      }
      taskProducerMapping.put(task, producer);
    }

    return taskProducerMapping;
  }
}
