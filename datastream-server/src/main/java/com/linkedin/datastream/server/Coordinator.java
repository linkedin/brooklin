package com.linkedin.datastream.server;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.commons.lang.Validate;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Meter;
import com.codahale.metrics.Metric;

import com.linkedin.datastream.common.Datastream;
import com.linkedin.datastream.common.DatastreamDestination;
import com.linkedin.datastream.common.DatastreamException;
import com.linkedin.datastream.common.DatastreamMetadataConstants;
import com.linkedin.datastream.common.DatastreamStatus;
import com.linkedin.datastream.common.DynamicMetricsManager;
import com.linkedin.datastream.common.ErrorLogger;
import com.linkedin.datastream.common.MetricsAware;
import com.linkedin.datastream.common.ReflectionUtils;
import com.linkedin.datastream.common.VerifiableProperties;
import com.linkedin.datastream.server.api.connector.Connector;
import com.linkedin.datastream.server.api.connector.DatastreamValidationException;
import com.linkedin.datastream.server.api.strategy.AssignmentStrategy;
import com.linkedin.datastream.server.api.transport.TransportProvider;
import com.linkedin.datastream.server.api.transport.TransportProviderFactory;
import com.linkedin.datastream.server.providers.CheckpointProvider;
import com.linkedin.datastream.server.providers.ZookeeperCheckpointProvider;
import com.linkedin.datastream.server.zk.ZkAdapter;


/**
 *
 * Coordinator is the object that bridges the ZooKeeper with Connector implementations. There is one instance
 * of Coordinator for each deployable DatastreamService instance. The Coordinator can connect multiple connectors,
 * but each of them must belong to different type. The Coordinator calls the Connector.getConnectorType() to
 * inspect the type of the connectors to make sure that there is only one connector for each type.
 *
 * <p> The zookeeper interactions wrapped in {@link ZkAdapter}, and depending on the state of the instance, it
 * emits callbacks:
 *
 * <ul>
 *     <li>{@link Coordinator#onBecomeLeader()} This callback is triggered when this instance becomes the
 *     leader of the Datastream cluster</li>
 *
 *     <li>{@link Coordinator#onDatastreamChange()} Only the Coordinator leader monitors the Datastream definitions
 *     in ZooKeeper. When there are changes made to datastream definitions through Datastream Management Service,
 *     this callback will be triggered on the Coordinator Leader so it can reassign datastream tasks among
 *     live instances..</li>
 *
 *     <li>{@link Coordinator#onLiveInstancesChange()} Only the Coordinator leader monitors the list of
 *     live instances in the cluster. If there are any instances go online or offline, this callback is triggered
 *     so the Coordinator leader can reassign datastream tasks among live instances.</li>
 *
 *     <li>{@link Coordinator#onAssignmentChange()} </li> All Coordinators, including the leader instance, will
 *     get notified if the datastream tasks assigned to it is updated through this callback. This is where
 *     the Coordinator can trigger the Connector API to notify corresponding connectors
 * </ul>
 *
 *
 *
 *                                         Coordinator                       Connector
 *
 * ┌──────────────┐       ┌─────────────────────────────────────────┐    ┌─────────────────┐
 * │              │       │                                         │    │                 │
 * │              │       │                                         │    │                 │
 * │              │       │ ┌──────────┐  ┌────────────────┐        │    │                 │
 * │              │       │ │ZkAdapter ├──▶ onBecomeLeader │        │    │                 │
 * │              │       │ │          │  └────────────────┘        │    │                 │
 * │              ├───────┼─▶          │  ┌──────────────────┐      │    │                 │
 * │              │       │ │          ├──▶ onBecomeFollower │      │    │                 │
 * │              │       │ │          │  └──────────────────┘      │    │                 │
 * │              │       │ │          │  ┌────────────────────┐    │    │                 │
 * │  ZooKeeper   ├───────┼─▶          ├──▶ onAssignmentChange ├────┼────▶                 │
 * │              │       │ │          │  └────────────────────┘    │    │                 │
 * │              │       │ │          │  ┌───────────────────────┐ │    │                 │
 * │              │       │ │          ├──▶ onLiveInstancesChange │ │    │                 │
 * │              ├───────┼─▶          │  └───────────────────────┘ │    │                 │
 * │              │       │ │          │                            │    │                 │
 * │              │       │ │          │                            │    │                 │
 * │              │       │ │          │                            │    │                 │
 * │              │       │ └──────────┘                            │    │                 │
 * │              │       │                                         │    │                 │
 * └──────────────┘       │                                         │    │                 │
 *                        └─────────────────────────────────────────┘    └─────────────────┘
 *
 */

public class Coordinator implements ZkAdapter.ZkAdapterListener, MetricsAware {
  private final CachedDatastreamReader _datastreamCache;
  private Logger _log = LoggerFactory.getLogger(Coordinator.class.getName());

  private static final long EVENT_THREAD_JOIN_TIMEOUT = 1000L;
  public static final String SCHEMA_REGISTRY_CONFIG_DOMAIN = "datastream.server.schemaRegistry";
  public static final String TRANSPORT_PROVIDER_CONFIG_DOMAIN = "datastream.server.transportProvider";
  public static final String EVENT_PRODUCER_CONFIG_DOMAIN = "datastream.server.eventProducer";

  private final CoordinatorEventBlockingQueue _eventQueue;
  private final CoordinatorEventProcessor _eventThread;
  private final EventProducerPool _eventProducerPool;
  private final ThreadPoolExecutor _assignmentChangeThreadPool;
  private final String _clusterName;
  private ScheduledExecutorService _executor = Executors.newSingleThreadScheduledExecutor();

  // make sure the scheduled retries are not duplicated
  AtomicBoolean leaderDatastreamAddOrDeleteEventScheduled = new AtomicBoolean(false);

  // make sure the scheduled retries are not duplicated
  AtomicBoolean leaderDoAssignmentScheduled = new AtomicBoolean(false);

  private final CoordinatorConfig _config;
  private final ZkAdapter _adapter;

  // mapping from connector name to associated assignment strategy
  private final Map<String, AssignmentStrategy> _strategies = new HashMap<>();

  // Connector types which has custom checkpointing enabled.
  private Set<String> _customCheckpointingConnectors = new HashSet<>();

  // mapping from connector type to connector instance
  private final Map<String, ConnectorWrapper> _connectors = new HashMap<>();

  private final TransportProvider _transportProvider;
  private final DestinationManager _destinationManager;

  // Currently assigned datastream tasks by taskName
  private Map<String, DatastreamTask> _assignedDatastreamTasks = new HashMap<>();

  private final Map<String, Metric> _metrics = new HashMap<>();
  private Meter _numRebalances = new Meter();
  private final DynamicMetricsManager _dynamicMetricsManager;
  private static final String NUM_ERRORS = "numErrors";
  private static final String NUM_RETRIES = "numRetries";

  public Coordinator(CachedDatastreamReader datastreamCache, Properties config) throws DatastreamException {
    this(datastreamCache, new CoordinatorConfig(config));
  }

  public Coordinator(CachedDatastreamReader datastreamCache, CoordinatorConfig config) throws DatastreamException {
    _datastreamCache = datastreamCache;
    _config = config;
    _clusterName = _config.getCluster();

    _adapter = new ZkAdapter(_config.getZkAddress(), _clusterName, _config.getZkSessionTimeout(),
        _config.getZkConnectionTimeout(), this, datastreamCache);
    _adapter.setListener(this);

    _eventQueue = new CoordinatorEventBlockingQueue();
    _eventThread = new CoordinatorEventProcessor();
    _eventThread.setDaemon(true);

    _dynamicMetricsManager = DynamicMetricsManager.getInstance();

    // Creating a separate thread pool for making the onAssignmentChange calls to the connector
    _assignmentChangeThreadPool = new ThreadPoolExecutor(config.getAssignmentChangeThreadPoolThreadCount(),
        config.getAssignmentChangeThreadPoolThreadCount(), 10, TimeUnit.SECONDS, new LinkedBlockingQueue<>());

    VerifiableProperties coordinatorProperties = new VerifiableProperties(_config.getConfigProperties());

    String transportFactory = config.getTransportProviderFactory();
    TransportProviderFactory factory = ReflectionUtils.createInstance(transportFactory);

    if (factory == null) {
      ErrorLogger.logAndThrowDatastreamRuntimeException(_log,
          "failed to create transport provider factory: " + transportFactory, null);
    }

    _transportProvider =
        factory.createTransportProvider(coordinatorProperties.getDomainProperties(TRANSPORT_PROVIDER_CONFIG_DOMAIN));
    if (_transportProvider == null) {
      ErrorLogger.logAndThrowDatastreamRuntimeException(_log,
          "failed to create transport provider, factory: " + transportFactory, null);
    }

    Optional.ofNullable(_transportProvider.getMetrics()).ifPresent(_metrics::putAll);

    _destinationManager = new DestinationManager(config.isReuseExistingDestination(), _transportProvider);

    CheckpointProvider cpProvider = new ZookeeperCheckpointProvider(_adapter);
    Optional.ofNullable(cpProvider.getMetrics()).ifPresent(m -> _metrics.putAll(m));

    _eventProducerPool = new EventProducerPool(cpProvider, factory,
        coordinatorProperties.getDomainProperties(TRANSPORT_PROVIDER_CONFIG_DOMAIN),
        coordinatorProperties.getDomainProperties(EVENT_PRODUCER_CONFIG_DOMAIN));
    Optional.ofNullable(_eventProducerPool.getMetrics()).ifPresent(m -> _metrics.putAll(m));
  }

  public void start() {
    _log.info("Starting coordinator");
    _eventThread.start();
    _adapter.connect();
    _log = LoggerFactory.getLogger(String.format("%s:%s", Coordinator.class.getName(), _adapter.getInstanceName()));

    for (String connectorType : _connectors.keySet()) {
      ConnectorWrapper connector = _connectors.get(connectorType);

      // populate the instanceName. We only know the instance name after _adapter.connect()
      connector.setInstanceName(getInstanceName());

      // make sure connector znode exists upon instance start. This way in a brand new cluster
      // we can inspect zookeeper and know what connectors are created
      _adapter.ensureConnectorZNode(connector.getConnectorType());

      // call connector::start API
      connector.start();

      _log.info("Coordinator started");
    }

    // now that instance is started, make sure it doesn't miss any assignment created during
    // the slow startup
    _eventQueue.put(CoordinatorEvent.createHandleAssignmentChangeEvent());
  }

  public void stop() {
    _log.info("Stopping coordinator");
    for (String connectorType : _connectors.keySet()) {
      try {
        _connectors.get(connectorType).stop();
      } catch (Exception ex) {
        _log.warn(String.format(
            "Connector stop threw an exception for connectorType %s, " + "Swallowing it and continuing shutdown.",
            connectorType), ex);
      }
    }

    while (_eventThread.isAlive()) {
      try {
        _eventThread.interrupt();
        _eventThread.join(EVENT_THREAD_JOIN_TIMEOUT);
      } catch (InterruptedException e) {
        _log.warn("Exception caught while stopping coordinator", e);
      }
    }

    _eventProducerPool.shutdown();

    _adapter.disconnect();
    _log.info("Coordinator stopped");
  }

  public String getInstanceName() {
    return _adapter.getInstanceName();
  }

  public Collection<DatastreamTask> getDatastreamTasks() {
    return _assignedDatastreamTasks.values();
  }

  /**
   * This method is called when the current datastream server instance becomes a leader.
   * There can only be only one leader in a datastream cluster.
   */
  @Override
  public void onBecomeLeader() {
    _log.info("Coordinator::onBecomeLeader is called");
    // when an instance becomes a leader, make sure we don't miss new datastreams and
    // new assignment tasks that was not finished by the previous leader
    _eventQueue.put(CoordinatorEvent.createHandleDatastreamAddOrDeleteEvent());
    _eventQueue.put(CoordinatorEvent.createLeaderDoAssignmentEvent());
    _log.info("Coordinator::onBecomeLeader completed successfully");
  }

  /**
   * This method is called when a new datastream server is added or existing datastream server goes down.
   */
  @Override
  public void onLiveInstancesChange() {
    _log.info("Coordinator::onLiveInstancesChange is called");
    _eventQueue.put(CoordinatorEvent.createLeaderDoAssignmentEvent());
    _log.info("Coordinator::onLiveInstancesChange completed successfully");
  }

  /**
   * This method is called when a new datastream is created. Right now we do not handle datastream updates/deletes.
   */
  @Override
  public void onDatastreamChange() {
    _log.info("Coordinator::onDatastreamChange is called");
    // if there are new datastreams created, we need to trigger the topic creation logic
    _eventQueue.put(CoordinatorEvent.createHandleDatastreamAddOrDeleteEvent());
    _eventQueue.put(CoordinatorEvent.createLeaderDoAssignmentEvent());
    _log.info("Coordinator::onDatastreamChange completed successfully");
  }

  /**
   *
   * This method is called when the coordinator is notified that there are datastreamtask assignment changes
   * for this instance. To handle this change, we need to take the following steps:
   * (1) get a list of all current assignment.
   * (2) inspect the task to find out which connectors are responsible for handling the changed assignment
   * (3) call corresponding connector API so that the connectors can handle the assignment changes.
   *
   */
  @Override
  public void onAssignmentChange() {
    _log.info("Coordinator::onAssignmentChange is called");
    _eventQueue.put(CoordinatorEvent.createHandleAssignmentChangeEvent());
    _log.info("Coordinator::onAssignmentChange completed successfully");
  }

  private void handleAssignmentChange() {
    long startAt = System.currentTimeMillis();

    // when there is any change to the assignment for this instance. Need to find out what is the connector
    // type of the changed assignment, and then call the corresponding callback of the connector instance
    List<String> assignment = _adapter.getInstanceAssignment(_adapter.getInstanceName());

    _log.info("START: Coordinator::handleAssignmentChange. Instance: " + _adapter.getInstanceName() + ", assignment: "
        + assignment);

    // all datastream tasks for all connector types
    Map<String, List<DatastreamTask>> currentAssignment = new HashMap<>();
    assignment.forEach(ds -> {

      DatastreamTask task = getDatastreamTask(ds);

      String connectorType = task.getConnectorType();
      if (!currentAssignment.containsKey(connectorType)) {
        currentAssignment.put(connectorType, new ArrayList<>());
      }
      currentAssignment.get(connectorType).add(task);
    });

    _log.info(printAssignmentByType(currentAssignment));

    //
    // diff the currentAssignment with last saved assignment _assignedDatastreamTasksByConnectorType and make sure
    // the affected connectors are notified through the callback. There are following cases:
    // (1) a connector is removed of all assignment. This means the connector type does not exist in
    //     currentAssignment, but exist in the previous assignment in _assignedDatastreamTasksByConnectorType
    // (2) there are any changes of assignment for an existing connector type, including datastreamtasks
    //     added or removed. We do not handle the case when datastreamtask is updated. This include the
    //     case a connector previously doesn't have assignment but now has. This means the connector type
    //     is not contained in currentAssignment, but contained in _assignedDatastreamTasksByConnectorType
    //

    // case (1), find connectors that now doesn't handle any tasks
    List<String> oldConnectorList = _assignedDatastreamTasks.values()
        .stream()
        .map(DatastreamTask::getConnectorType)
        .distinct()
        .collect(Collectors.toList());
    List<String> newConnectorList = new ArrayList<>();
    newConnectorList.addAll(currentAssignment.keySet());

    List<String> deactivated = new ArrayList<>(oldConnectorList);
    deactivated.removeAll(newConnectorList);
    List<Future<Void>> assignmentChangeFutures = deactivated.stream()
        .map(connectorType -> dispatchAssignmentChangeIfNeeded(connectorType, new ArrayList<>()))
        .filter(Objects::nonNull)
        .collect(Collectors.toList());

    // case (2) - Dispatch all the assignment changes in a separate thread
    assignmentChangeFutures.addAll(newConnectorList.stream()
        .map(connectorType -> dispatchAssignmentChangeIfNeeded(connectorType, currentAssignment.get(connectorType)))
        .filter(Objects::nonNull)
        .collect(Collectors.toList()));

    // Wait till all the futures are complete.
    for (Future<Void> assignmentChangeFuture : assignmentChangeFutures) {
      try {
        assignmentChangeFuture.get();
      } catch (InterruptedException e) {
        _log.warn("onAssignmentChange call got interrupted", e);
        break;
      } catch (ExecutionException e) {
        _log.warn("onAssignmentChange call threw exception", e);
      }
    }

    // now save the current assignment
    _assignedDatastreamTasks.clear();
    _assignedDatastreamTasks = currentAssignment.values()
        .stream()
        .flatMap(Collection::stream)
        .collect(Collectors.toMap(DatastreamTask::getDatastreamTaskName, Function.identity()));

    long endAt = System.currentTimeMillis();

    _log.info(String.format("END: Coordinator::handleAssignmentChange, Duration: %d milliseconds", endAt - startAt));
  }

  private DatastreamTask getDatastreamTask(String taskName) {
    if (_assignedDatastreamTasks.containsKey(taskName)) {
      return _assignedDatastreamTasks.get(taskName);
    } else {
      return _adapter.getAssignedDatastreamTask(_adapter.getInstanceName(), taskName);
    }
  }

  private Future<Void> dispatchAssignmentChangeIfNeeded(String connectorType, List<DatastreamTask> assignment) {
    ConnectorWrapper connector = _connectors.get(connectorType);

    List<DatastreamTask> addedTasks = new ArrayList<>(assignment);
    List<DatastreamTask> removedTasks;
    List<DatastreamTask> oldAssignment = _assignedDatastreamTasks.values()
        .stream()
        .filter(t -> t.getConnectorType().equals(connectorType))
        .collect(Collectors.toList());

    // if there are any difference in the list of assignment. Note that if there are no difference
    // between the two lists, then the connector onAssignmentChange() is not called.
    addedTasks.removeAll(oldAssignment);
    oldAssignment.removeAll(assignment);
    removedTasks = oldAssignment;

    if (!addedTasks.isEmpty() || !removedTasks.isEmpty()) {
      // Populate the event producers before calling the connector with the list of tasks.
      _eventProducerPool.assignEventProducers(connectorType, addedTasks, removedTasks,
          _customCheckpointingConnectors.contains(connectorType));

      // Dispatch the onAssignmentChange to the connector in a separate thread.
      return _assignmentChangeThreadPool.submit((Callable<Void>) () -> {
        try {
          connector.onAssignmentChange(assignment);

          // Unassign tasks with producers
          _eventProducerPool.unassignEventProducers(removedTasks);
        } catch (Exception ex) {
          _log.warn(String.format("connector.onAssignmentChange for connector %s threw an exception, "
              + "Queuing up a new onAssignmentChange event for retry.", connectorType), ex);
          _eventQueue.put(CoordinatorEvent.createHandleInstanceErrorEvent(ExceptionUtils.getRootCauseMessage(ex)));
          _eventQueue.put(CoordinatorEvent.createHandleAssignmentChangeEvent());
        }

        return null;
      });
    }

    return null;
  }

  protected synchronized void handleEvent(CoordinatorEvent event) {
    _log.info("START: Handle event " + event.getType() + ", Instance: " + _adapter.getInstanceName());

    try {
      switch (event.getType()) {
        case LEADER_DO_ASSIGNMENT:
          handleLeaderDoAssignment();
          break;

        case HANDLE_ASSIGNMENT_CHANGE:
          handleAssignmentChange();
          break;

        case HANDLE_ADD_OR_DELETE_DATASTREAM:
          handleDatastreamAddOrDelete();
          break;

        case HANDLE_INSTANCE_ERROR:
          handleInstanceError((CoordinatorEvent.HandleInstanceError) event);
          break;
        default:
          String errorMessage = String.format("Unknown event type %s.", event.getType());
          ErrorLogger.logAndThrowDatastreamRuntimeException(_log, errorMessage, null);
          break;
      }
    } catch (Exception e) {
      _dynamicMetricsManager.createOrUpdateMeter(this.getClass(), "handleEvent-" + event.getType(), NUM_ERRORS, 1);
      _log.error("ERROR: event + " + event + " failed.", e);
    }

    _log.info("END: Handle event " + event);
  }

  // when we encounter an error, we need to persist the error message in zookeeper. We only persist the
  // first 10 messages. Why we put this logic in event loop instead of synchronously handle it? This
  // is because the same reason that can result in error can also result in the failure of persisting
  // the error message.
  private void handleInstanceError(CoordinatorEvent.HandleInstanceError event) {
    String msg = event.getEventData();
    _adapter.zkSaveInstanceError(msg);
  }

  // when there are new datastreams defined in DSM, we need to decide its target from the corresponding
  // connector, and write back the target to dsm tree in zookeeper. The assumption is that we only
  // detect the target of a datastream when it is first added. We do not handle the case at this point
  // when the datastream definition can be updated in DSM.
  private void handleDatastreamAddOrDelete() {
    boolean shouldRetry = false;
    // Allow further retry scheduling
    leaderDatastreamAddOrDeleteEventScheduled.set(false);

    // Get the list of all datastreams
    List<Datastream> newDatastreams = _datastreamCache.getAllDatastreams(true);

    // do nothing if there are zero datastreams
    if (newDatastreams.isEmpty()) {
      _log.warn("Received a new datastream event, but there were no datastreams");
      return;
    }

    for (Datastream ds : newDatastreams) {
      if (ds.getStatus() != DatastreamStatus.READY) {
        try {
          _destinationManager.createTopic(ds);

          // Set the datastream status as ready for use (both producing and consumption)
          ds.setStatus(DatastreamStatus.READY);
          if (!_adapter.updateDatastream(ds)) {
            _log.warn(String.format("Failed to update datastream: %s after initializing, "
                + "This datastream will not be scheduled for producing events ", ds.getName()));
            shouldRetry = true;
          }
        } catch (Exception e) {
          _log.warn("Failed to update the destination of new datastream %s " + ds, e);
          shouldRetry = true;
        }
      }
    }

    if (shouldRetry) {
      _dynamicMetricsManager.createOrUpdateMeter(this.getClass(), "handleDatastreamAddOrDelete", NUM_RETRIES, 1);

      // If there are any failure, we will need to schedule retry if
      // there is no pending retry scheduled already.
      if (leaderDatastreamAddOrDeleteEventScheduled.compareAndSet(false, true)) {
        _log.warn("Schedule retry for handling new datastream");
        _executor.schedule(() -> _eventQueue.put(CoordinatorEvent.createHandleDatastreamAddOrDeleteEvent()),
            _config.getRetryIntervalMS(), TimeUnit.MILLISECONDS);
      }
    }

    _eventQueue.put(CoordinatorEvent.createLeaderDoAssignmentEvent());
  }

  // Compare two datastreams based on creation timestamp ("" is smaller)
  private static int compareCreationMs(Datastream d1, Datastream d2) {
    String ts1 = d1.getMetadata().getOrDefault(DatastreamMetadataConstants.CREATION_MS, "");
    String ts2 = d2.getMetadata().getOrDefault(DatastreamMetadataConstants.CREATION_MS, "");
    return ts1.compareTo(ts2);
  }

  private void handleLeaderDoAssignment() {

    // get all current live instances
    List<String> liveInstances = _adapter.getLiveInstances();

    // Get all streams that are assignable. Assignable datastreams are the ones:
    //  1) has a valid destination
    //  2) status is READY
    //
    // TODO: currently datastreams are sorted based on the creation timestamp
    //       before de-duping. This avoid creating duplicate tasks for a newer
    //       datastream sharing destination with an existing datastream but
    //       name appears before the older datastream because of alphabetic
    //       ordering from zookeper. We might need to consider a long-term
    //       solution.
    List<Datastream> allStreams = _datastreamCache.getAllDatastreams()
        .stream()
        .filter(datastream -> datastream.hasStatus()
            && datastream.getStatus() == DatastreamStatus.READY
            && datastream.hasDestination()
            && datastream.getDestination().hasConnectionString())
        .sorted((d1, d2) -> compareCreationMs(d1, d2))
        .collect(Collectors.toList());

    // The inner map is used to dedup Datastreams with the same destination
    Map<String, Map<DatastreamDestination, Datastream>> streamsByConnectorType = new HashMap<>();

    for (Datastream ds : allStreams) {
      Map<DatastreamDestination, Datastream> streams = streamsByConnectorType.getOrDefault(ds.getConnectorName(), null);
      if (streams == null) {
        streams = new HashMap<>();
        streamsByConnectorType.put(ds.getConnectorName(), streams);
      }

      // Only keep the datastreams with unique destinations
      if (!streams.containsKey(ds.getDestination())) {
        streams.put(ds.getDestination(), ds);
      } else {
        _log.debug(String.format("Datastream %s is de-duped by %s", ds, streams.get(ds.getDestination())));
      }
    }

    _log.debug("handleLeaderDoAssignment: final datastreams for task assignment: " + streamsByConnectorType);

    // Map between Instance and the tasks
    Map<String, List<DatastreamTask>> assignmentsByInstance = new HashMap<>();
    Map<String, Set<DatastreamTask>> currentAssignment = _adapter.getAllAssignedDatastreamTasks();

    _log.info("handleLeaderDoAssignment: current assignment: " + currentAssignment);

    for (String connectorType : streamsByConnectorType.keySet()) {
      AssignmentStrategy strategy = _strategies.get(connectorType);
      List<Datastream> datastreamsPerConnectorType =
          new ArrayList<>(streamsByConnectorType.get(connectorType).values());

      // Get the list of tasks per instance for the given connector type
      Map<String, Set<DatastreamTask>> tasksByConnectorAndInstance =
          strategy.assign(datastreamsPerConnectorType, liveInstances, currentAssignment);

      for (String instance : tasksByConnectorAndInstance.keySet()) {
        if (!assignmentsByInstance.containsKey(instance)) {
          assignmentsByInstance.put(instance, new ArrayList<>());
        }
        // Add the tasks for this connector type to the instance
        tasksByConnectorAndInstance.get(instance).forEach(task -> {
          // Each task must have a valid zkAdapter
          ((DatastreamTaskImpl) task).setZkAdapter(_adapter);
          assignmentsByInstance.get(instance).add(task);
        });
      }
    }

    // persist the assigned result to zookeeper. This means we will need to compare with the current
    // assignment and do remove and add znodes accordingly. In the case of zookeeper failure (when
    // it failed to create or delete znodes), we will do our best to continue the current process
    // and schedule a retry. The retry should be able to diff the remaining zookeeper work
    boolean succeeded = true;
    for (Map.Entry<String, List<DatastreamTask>> entry : assignmentsByInstance.entrySet()) {
      succeeded &= _adapter.updateInstanceAssignment(entry.getKey(), entry.getValue());
    }

    _log.info("handleLeaderDoAssignment: new assignment: " + assignmentsByInstance);

    // clean up tasks under dead instances if everything went well
    if (succeeded) {
      _adapter.cleanupDeadInstanceAssignments(currentAssignment);
      _numRebalances.mark();
    }

    // schedule retry if failure
    if (!succeeded && !leaderDoAssignmentScheduled.get()) {
      _log.info("Schedule retry for leader assigning tasks");
      _dynamicMetricsManager.createOrUpdateMeter(this.getClass(), "handleLeaderDoAssignment", NUM_RETRIES, 1);
      leaderDoAssignmentScheduled.set(true);
      _executor.schedule(() -> {
        _eventQueue.put(CoordinatorEvent.createLeaderDoAssignmentEvent());
        leaderDoAssignmentScheduled.set(false);
      }, _config.getRetryIntervalMS(), TimeUnit.MILLISECONDS);
    }
  }

  /**
   * Add a connector to the coordinator. A coordinator can handle multiple type of connectors, but only one
   * connector per connector type.
   *
   * @param connectorName of the connector.
   * @param connector a connector that implements the Connector interface
   * @param strategy the assignment strategy deciding how to distribute datastream tasks among instances
   * @param customCheckpointing whether connector uses custom checkpointing. if the custom checkpointing is set to true
   *                            Coordinator will not perform checkpointing to the zookeeper.
   */
  public void addConnector(String connectorName, Connector connector, AssignmentStrategy strategy,
      boolean customCheckpointing) {

    Validate.notNull(strategy, "strategy cannot be null");
    Validate.notEmpty(connectorName, "connectorName cannot be empty");
    Validate.notNull(connector, "Connector cannot be null");

    _log.info(String.format("Add new connector of type %s, strategy %s with custom checkpointing %s to coordinator",
        connectorName, strategy.getClass().getTypeName(), customCheckpointing));

    if (_connectors.containsKey(connectorName)) {
      String err = "A connector of type " + connectorName + " already exists.";
      _log.error(err);
      throw new IllegalArgumentException(err);
    }

    Optional<Map<String, Metric>> connectorMetrics = Optional.ofNullable(connector.getMetrics());
    connectorMetrics.ifPresent(m -> _metrics.putAll(m));

    ConnectorWrapper connectorWrapper = new ConnectorWrapper(connectorName, connector);
    _connectors.put(connectorName, connectorWrapper);
    _strategies.put(connectorName, strategy);
    if (customCheckpointing) {
      _customCheckpointingConnectors.add(connectorName);
    }
  }

  /**
   * initializes the datastream. Datastream management service will call this before writing the
   * Datastream into zookeeper. This method should ensure that the source has sufficient details.
   * @param datastream datastream for validation
   * @return result of the validation
   */
  public void initializeDatastream(Datastream datastream) throws DatastreamValidationException {
    datastream.setStatus(DatastreamStatus.INITIALIZING);
    String connectorName = datastream.getConnectorName();
    ConnectorWrapper connector = _connectors.get(connectorName);
    if (connector == null) {
      String errorMessage = "Invalid connector: " + connectorName;
      _log.error(errorMessage);
      throw new DatastreamValidationException(errorMessage);
    }

    List<Datastream> allDatastreams = _datastreamCache.getAllDatastreams()
        .stream()
        .filter(d -> d.getConnectorName().equals(connectorName))
        .collect(Collectors.toList());

    try {
      _log.debug(String.format("About to initialize datastream %s with connector %s", datastream, connectorName));
      connector.initializeDatastream(datastream, allDatastreams);
    } catch (Exception e) {
      _dynamicMetricsManager.createOrUpdateMeter(this.getClass(), "initializeDatastream", NUM_ERRORS, 1);
      throw e;
    }

    datastream.getMetadata().put(DatastreamMetadataConstants.CREATION_MS, String.valueOf(Instant.now().toEpochMilli()));
    _destinationManager.populateDatastreamDestination(datastream, allDatastreams);
  }

  @Override
  public Map<String, Metric> getMetrics() {
    _metrics.put(buildMetricName("numRebalances"), _numRebalances);

    // dynamic metrics for capturing various errors
    _metrics.put(getDynamicMetricPrefixRegex() + NUM_ERRORS, new Meter());
    _metrics.put(getDynamicMetricPrefixRegex() + NUM_RETRIES, new Meter());

    return Collections.unmodifiableMap(_metrics);
  }

  /**
   * @return the datastream clusterName
   */
  public String getClusterName() {
    return _clusterName;
  }

  private class CoordinatorEventProcessor extends Thread {
    @Override
    public void run() {
      _log.info("START CoordinatorEventProcessor thread");
      while (!isInterrupted()) {
        try {
          CoordinatorEvent event = _eventQueue.take();
          if (event != null) {
            handleEvent(event);
          }
        } catch (InterruptedException e) {
          _log.warn("CoordinatorEventProcessor interrupted", e);
          interrupt();
        } catch (Exception t) {
          _log.error("CoordinatorEventProcessor failed", t);
        }
      }
      _log.info("END CoordinatorEventProcessor");
    }
  }

  // helper method for logging
  private String printAssignmentByType(Map<String, List<DatastreamTask>> assignment) {
    StringBuilder sb = new StringBuilder();
    sb.append("Current assignment for instance: " + getInstanceName() + ":\n");
    for (Map.Entry<String, List<DatastreamTask>> entry : assignment.entrySet()) {
      sb.append(entry.getKey()).append(": ").append(entry.getValue()).append("\n");
    }
    // remove the final "\n"
    String result = sb.toString();
    return result.substring(0, result.length() - 1);
  }
}
