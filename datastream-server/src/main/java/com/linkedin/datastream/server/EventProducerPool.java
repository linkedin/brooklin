package com.linkedin.datastream.server;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.commons.lang.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Metric;

import com.linkedin.datastream.common.MetricsAware;
import com.linkedin.datastream.server.api.transport.TransportProvider;
import com.linkedin.datastream.server.api.transport.TransportProviderFactory;
import com.linkedin.datastream.server.providers.CheckpointProvider;


/**
 * This class manages creation of EventProducers for tasks per connector.
 * The coordinator uses this to create producers before passing it on to the connectors
 */
public class EventProducerPool implements MetricsAware {
  private static final Logger LOG = LoggerFactory.getLogger(EventProducerPool.class);
  private static final String DEFAULT_POOL_SIZE = "10";
  private final int _poolSize;

  private Map<DatastreamTask, EventProducer> _taskEventProducerMap;

  private final Map<Boolean, List<EventProducer>> _producerPool;
  private final CheckpointProvider _checkpointProvider;
  private final TransportProviderFactory _transportProviderFactory;
  private final Properties _transportProviderConfig;
  private final Properties _eventProducerConfig;
  private final Random _random;

  public static final String POOL_SIZE = "poolSize";

  private final Counter _unrecoverableErrors;

  public EventProducerPool(CheckpointProvider checkpointProvider, TransportProviderFactory transportProviderFactory,
      Properties transportProviderConfig, Properties eventProducerConfig) {

    Validate.notNull(checkpointProvider, "null checkpoint provider");
    Validate.notNull(transportProviderFactory, "null transport provider factory");
    Validate.notNull(transportProviderConfig, "null transport provider config");
    Validate.notNull(eventProducerConfig, "null event producer config");

    _checkpointProvider = checkpointProvider;
    _transportProviderFactory = transportProviderFactory;
    _transportProviderConfig = transportProviderConfig;
    _eventProducerConfig = eventProducerConfig;

    _poolSize = Integer.parseInt(eventProducerConfig.getProperty(POOL_SIZE, DEFAULT_POOL_SIZE));
    _producerPool = new HashMap<>();
    _taskEventProducerMap = new HashMap<>();
    _random = new Random();

    _unrecoverableErrors = new Counter();
  }

  /**
   * This method is called when the coordinator is assigned new datastream tasks
   * and is used to retrieve DatastreamEventProducer corresponding to the assigned tasks
   * @param connectorType type of connector.
   * @param customCheckpointing  decides whether custom checkpointing needs to be used or datastream server provided
   *                             checkpointing.
   *
   * @return map of task to event producer mapping for this connector type
   */
  public synchronized void assignEventProducers(String connectorType, List<DatastreamTask> addedTasks,
      List<DatastreamTask> removedTasks, boolean customCheckpointing) {

    Validate.notNull(connectorType);
    Validate.notEmpty(connectorType);

    if (!_producerPool.containsKey(customCheckpointing)) {
      _producerPool.put(customCheckpointing, createProducers(_poolSize / 2, customCheckpointing));
    }

    List<EventProducer> producers = _producerPool.get(customCheckpointing);
    for (int addedTaskIndex = 0; addedTaskIndex < addedTasks.size(); addedTaskIndex++) {
      DatastreamTask task = addedTasks.get(addedTaskIndex);
      EventProducer eventProducer;
      if (addedTaskIndex < removedTasks.size()) {
        eventProducer =
            ((DatastreamEventProducerImpl) removedTasks.get(addedTaskIndex).getEventProducer()).getEventProducer();
      } else {
        eventProducer = producers.get(_random.nextInt(producers.size()));
      }

      assignEventProducerToTask(eventProducer, task);
    }

    // NOTE: removedTasks are unassigned later in unassignEventProducers
    // This is because at this time the connector might still be actively
    // sending events with the producer. If we unassign them right now,
    // their checkpoints will not get acknowledged in the producer. Thus,
    // we must unassign producers ONLY after connector.onAssignmentChange()
    // have returned.
  }

  private void assignEventProducerToTask(EventProducer eventProducer, DatastreamTask task) {
    eventProducer.assignTask(task);
    // If the task has the producer object, then just reset the underlying event producer
    DatastreamEventProducerImpl datastreamEventProducer = (DatastreamEventProducerImpl) task.getEventProducer();

    if (datastreamEventProducer == null) {
      ((DatastreamTaskImpl) task).setEventProducer(new DatastreamEventProducerImpl(task, eventProducer));
    } else {
      datastreamEventProducer.resetEventProducer(eventProducer);
    }

    _taskEventProducerMap.put(task, eventProducer);
  }

  /**
   * Detach producers with the unassign tasks.
   * @param unassignedTasks
   */
  public synchronized void unassignEventProducers(List<DatastreamTask> unassignedTasks) {
    Validate.notNull(unassignedTasks);
    unassignedTasks.forEach(t -> _taskEventProducerMap.get(t).unassignTask(t));
  }

  private List<EventProducer> createProducers(int poolSize, boolean customCheckpointing) {
    return IntStream.range(0, poolSize).mapToObj(i -> {
      // Each distinct destination has its own transport provider
      TransportProvider transport = _transportProviderFactory.createTransportProvider(_transportProviderConfig);
      return new EventProducer(transport, _checkpointProvider, _eventProducerConfig, customCheckpointing,
          this::onUnrecoverableError, 0);
    }).collect(Collectors.toList());
  }

  /**
   * On unrecoverable error we shutdown the existing producer and create a new producer and assign them to all the
   * tasks.
   */
  private synchronized void onUnrecoverableError(EventProducer eventProducer) {

    List<DatastreamTask> tasks = findTasksUsingEventProducer(eventProducer);

    LOG.warn(String.format("Producer-%d (%s) failed with unrecoverable error, creating a new producer and assigning"
        + " it to existing tasks %s. Bad producer will be shutdown.", eventProducer.getProducerId(), eventProducer,
        tasks));

    boolean customCheckpointing = eventProducer.getCheckpointPolicy() == EventProducer.CheckpointPolicy.CUSTOM;

    _producerPool.get(customCheckpointing).remove(eventProducer);

    TransportProvider transport = _transportProviderFactory.createTransportProvider(_transportProviderConfig);
    EventProducer newEventProducer =
        new EventProducer(transport, _checkpointProvider, _eventProducerConfig, customCheckpointing,
            this::onUnrecoverableError, eventProducer.getGeneration() + 1);

    tasks.forEach(t -> assignEventProducerToTask(newEventProducer, t));

    _unrecoverableErrors.inc();
  }

  private List<DatastreamTask> findTasksUsingEventProducer(EventProducer eventProducer) {
    return _taskEventProducerMap.entrySet()
        .stream()
        .filter(x -> x.getValue().equals(eventProducer))
        .map(Map.Entry::getKey)
        .collect(Collectors.toList());
  }

  /**
   * Shutdown all outstanding event producers. This should only be called by Coordinator.shutdown()
   */
  public synchronized void shutdown() {
    LOG.info("Shutting down all producers in event producer pool");
    if (_producerPool.get(true) != null) {
      _producerPool.get(true).forEach(EventProducer::shutdown);
    }

    if (_producerPool.get(false) != null) {
      _producerPool.get(false).forEach(EventProducer::shutdown);
    }

    _producerPool.clear();
  }

  @Override
  public Map<String, Metric> getMetrics() {
    Map<String, Metric> metrics = new HashMap<>();

    metrics.put(buildMetricName("unrecoverableErrors"), _unrecoverableErrors);
    Optional.ofNullable(EventProducer.getMetrics()).ifPresent(m -> metrics.putAll(m));

    return Collections.unmodifiableMap(metrics);
  }
}
