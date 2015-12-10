package com.linkedin.datastream.server;

import java.util.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class manages the DatastreamEventProducers for the Coordinator.
 */
public class EventProducerPool {

    // Map between Destination URI/Topic and the producer used to send messages to the topic
    private final Map<String, EventProducer> _producers = new HashMap<String, EventProducer>();
    private final EventProducerFactory _producerFactory;


    private static final Logger LOG = LoggerFactory.getLogger(Coordinator.class.getName());

    /**
    @param producerFactory is used to create the event producers.
     */
    public EventProducerPool(EventProducerFactory producerFactory) {
        Objects.requireNonNull(producerFactory);
        _producerFactory = producerFactory;
    }

    /**
     *
     * This method is called when the coordinator is assigned new datastream tasks
     * and is used to retrieve EventProducer corresponding to the assigned tasks
     * @param tasks list of datastream tasks
     * @return map of task to message producer
     */
    public synchronized Map<DatastreamTask, EventProducer> getEventProducers(List<DatastreamTask> tasks) {

        Objects.requireNonNull(tasks);

        // Mapping between the task and the producer.
        Map<DatastreamTask, EventProducer> taskProducerMapping = new HashMap<DatastreamTask, EventProducer>();

        // Tasks for which there is no producer in the pool
        List<DatastreamTask> unmappedTasks = new ArrayList<DatastreamTask>();

        // List of producers that don't have a corresponding task
        Map<String, EventProducer> unusedProducers = new HashMap<String, EventProducer>(_producers);

        // Check if we can reuse existing Datastream EventProducers
        tasks.forEach(task -> {
            String destination = task.getDatastream().getDestination().getConnectionString();
            if (_producers.get(destination) != null) {
                // TODO: the task associated with the producer may no longer be valid. Need a way to update producer association or way to decouple task from producer
                taskProducerMapping.put(task, _producers.get(destination));
                unusedProducers.remove(destination);
            } else {
                unmappedTasks.add(task);
            }
        });

        // Remove the unused producers from the producer pool.
        // TODO: We can clean up only when we exceed a certain threshold and hold on to message producers
        unusedProducers.forEach((k, v) -> _producers.remove(k));
        // TODO: Call producer shutdown once event producer interface is ready
        unusedProducers.clear();

        if (unmappedTasks.size() == 0) {
            // all tasks are mapped to a producer.
            return taskProducerMapping;
        }

        // Create new producers for unmapped tasks
        unmappedTasks.forEach(task -> {
            String destination = task.getDatastream().getDestination().getConnectionString();

            EventProducer producer = _producers.get(destination);
            if (producer == null) {
                LOG.info("Creating new message producer for " + destination);
                producer = _producerFactory.create(task);
                _producers.put(destination, producer);
            }
            else
            {
                // We dont expect multiple tasks to have the same destination in Espresso.
                LOG.error("Multiple tasks are sharing the same destination");
            }
            taskProducerMapping.put(task, producer);
        });

        return taskProducerMapping;

    }
}

