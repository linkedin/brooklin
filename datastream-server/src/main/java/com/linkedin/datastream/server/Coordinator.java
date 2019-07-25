/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.server;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BooleanSupplier;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.Validate;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.ImmutableList;

import com.linkedin.datastream.common.Datastream;
import com.linkedin.datastream.common.DatastreamAlreadyExistsException;
import com.linkedin.datastream.common.DatastreamConstants;
import com.linkedin.datastream.common.DatastreamDestination;
import com.linkedin.datastream.common.DatastreamException;
import com.linkedin.datastream.common.DatastreamMetadataConstants;
import com.linkedin.datastream.common.DatastreamPartitionsMetadata;
import com.linkedin.datastream.common.DatastreamStatus;
import com.linkedin.datastream.common.DatastreamTransientException;
import com.linkedin.datastream.common.DatastreamUtils;
import com.linkedin.datastream.common.ErrorLogger;
import com.linkedin.datastream.common.VerifiableProperties;
import com.linkedin.datastream.metrics.BrooklinCounterInfo;
import com.linkedin.datastream.metrics.BrooklinGaugeInfo;
import com.linkedin.datastream.metrics.BrooklinMeterInfo;
import com.linkedin.datastream.metrics.BrooklinMetricInfo;
import com.linkedin.datastream.metrics.DynamicMetricsManager;
import com.linkedin.datastream.metrics.MetricsAware;
import com.linkedin.datastream.serde.SerDe;
import com.linkedin.datastream.serde.SerDeSet;
import com.linkedin.datastream.server.api.connector.Connector;
import com.linkedin.datastream.server.api.connector.DatastreamDeduper;
import com.linkedin.datastream.server.api.connector.DatastreamValidationException;
import com.linkedin.datastream.server.api.security.AuthorizationException;
import com.linkedin.datastream.server.api.security.Authorizer;
import com.linkedin.datastream.server.api.serde.SerdeAdmin;
import com.linkedin.datastream.server.api.strategy.AssignmentStrategy;
import com.linkedin.datastream.server.api.transport.TransportException;
import com.linkedin.datastream.server.api.transport.TransportProvider;
import com.linkedin.datastream.server.api.transport.TransportProviderAdmin;
import com.linkedin.datastream.server.assignment.StickyPartitionAssignmentStrategy;
import com.linkedin.datastream.server.providers.CheckpointProvider;
import com.linkedin.datastream.server.providers.ZookeeperCheckpointProvider;
import com.linkedin.datastream.server.zk.ZkAdapter;

import static com.linkedin.datastream.common.DatastreamMetadataConstants.CREATION_MS;
import static com.linkedin.datastream.common.DatastreamMetadataConstants.SYSTEM_DESTINATION_PREFIX;
import static com.linkedin.datastream.common.DatastreamMetadataConstants.TTL_MS;
import static com.linkedin.datastream.common.DatastreamUtils.hasValidDestination;
import static com.linkedin.datastream.common.DatastreamUtils.isReuseAllowed;


/**
 *
 * Coordinator is the object that bridges ZooKeeper with Connector implementations. There is one instance
 * of Coordinator for each deployable Brooklin service instance. The Coordinator can connect multiple connectors,
 * but each of them must belong to a different type. The Coordinator calls the Connector.getConnectorType() to
 * inspect the type of the connectors to make sure that there is only one connector for each type.
 *
 * <p> ZooKeeper interactions wrapped in {@link ZkAdapter}, and depending on the state of the instance, it
 * emits callbacks:
 *
 * <ul>
 *     <li>{@link Coordinator#onBecomeLeader()} This callback is triggered when this instance becomes the
 *     leader of the Datastream cluster</li>
 *
 *     <li>{@link Coordinator#onDatastreamAddOrDrop()} Only the Coordinator leader monitors the Datastream definitions
 *     in ZooKeeper. When there are changes made to datastream definitions through Datastream Management Service,
 *     this callback will be triggered on the Coordinator Leader so it can reassign datastream tasks among
 *     live instances.</li>
 *
 *     <li>{@link Coordinator#onLiveInstancesChange()} Only the Coordinator leader monitors the list of
 *     live instances in the cluster. If there are any instances go online or offline, this callback is triggered
 *     so the Coordinator leader can reassign datastream tasks among live instances.</li>
 *
 *     <li>{@link Coordinator#onDatastreamUpdate()} This callback is triggered when any updates have been made
 *     to existing datastreams to schedule an AssignmentChange event for task reassignment if necessary. </li>
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
 * │              │       │ │          │  ┌────────────────────┐    │    │                 │
 * │              │       │ │          ├──▶ onDatastreamUpdate ├────┼────▶                 │
 * │              │       │ │          │  └────────────────────┘    │    │                 │
 * │              │       │ └──────────┘                            │    │                 │
 * │              │       │                                         │    │                 │
 * └──────────────┘       │                                         │    │                 │
 *                        └─────────────────────────────────────────┘    └─────────────────┘
 *
 */
public class Coordinator implements ZkAdapter.ZkAdapterListener, MetricsAware {
  /*
     There are situation where we need to pause a Datastream without taking down the Brooklin Server.
     For example to temporary stop a misbehaving datastream, or to fix some connectivity issues.

     The coordinator reassign the tasks of the paused datastream to a dummy instance  "PAUSED_INSTANCE",
     effectively suspending processing of the current tasks.
       - In case a datastream is deduped, the tasks are reassigned only if all the datastreams are paused.
       - The tasks status are changed from OK to Paused.
   */
  public static final String PAUSED_INSTANCE = "PAUSED_INSTANCE";
  private static final String EVENT_PRODUCER_CONFIG_DOMAIN = "brooklin.server.eventProducer";

  private static final String MODULE = Coordinator.class.getSimpleName();
  private static final long EVENT_THREAD_JOIN_TIMEOUT = 1000L;
  private static final Duration ASSIGNMENT_TIMEOUT = Duration.ofSeconds(30);
  private static final String NUM_REBALANCES = "numRebalances";
  private static final String NUM_ERRORS = "numErrors";
  private static final String NUM_RETRIES = "numRetries";
  private static final String NUM_HEARTBEATS = "numHeartbeats";
  private static final String NUM_ASSIGNMENT_CHANGES = "numAssignmentChanges";
  private static final String NUM_PARTITION_ASSIGNMENTS = "numPartitionAssignments";
  private static final String NUM_PARTITION_MOVEMENTS = "numPartitionMovements";
  private static final String NUM_PAUSED_DATASTREAMS_GROUPS = "numPausedDatastreamsGroups";
  private static final String MAX_PARTITION_COUNT_IN_TASK = "maxPartitionCountInTask";
  private static final String IS_LEADER = "isLeader";

  // Connector common metrics
  private static final String NUM_DATASTREAMS = "numDatastreams";
  private static final String NUM_DATASTREAM_TASKS = "numDatastreamTasks";

  private static AtomicLong _pausedDatastreamsGroups = new AtomicLong(0L);

  private final CachedDatastreamReader _datastreamCache;
  private final Properties _eventProducerConfig;
  private final CheckpointProvider _cpProvider;
  private final Map<String, TransportProviderAdmin> _transportProviderAdmins = new HashMap<>();
  private final CoordinatorEventBlockingQueue _eventQueue;
  private final CoordinatorEventProcessor _eventThread;
  private final ThreadPoolExecutor _assignmentChangeThreadPool;
  private final String _clusterName;
  private final CoordinatorConfig _config;
  private final ZkAdapter _adapter;

  // mapping from connector type to connector Info instance
  private final Map<String, ConnectorInfo> _connectors = new HashMap<>();

  // Currently assigned datastream tasks by taskName
  private final Map<String, DatastreamTask> _assignedDatastreamTasks = new ConcurrentHashMap<>();

  private final List<BrooklinMetricInfo> _metrics = new ArrayList<>();
  private final DynamicMetricsManager _dynamicMetricsManager;

  // One coordinator heartbeat per minute, heartbeat helps detect dead/live-lock
  // where no events can be handled if coordinator locks up. This can happen because
  // handleEvent is synchronized and downstream code can misbehave.
  private final Duration _heartbeatPeriod;

  private final Logger _log = LoggerFactory.getLogger(Coordinator.class.getName());
  private final ScheduledExecutorService _executor = Executors.newSingleThreadScheduledExecutor();

  // make sure the scheduled retries are not duplicated
  private final AtomicBoolean leaderDatastreamAddOrDeleteEventScheduled = new AtomicBoolean(false);

  // make sure the scheduled retries are not duplicated
  private final AtomicBoolean leaderDoAssignmentScheduled = new AtomicBoolean(false);

  // make sure the scheduled retries are not duplicated
  private final AtomicBoolean leaderPartitionAssignmentScheduled = new AtomicBoolean(false);


  private final Map<String, SerdeAdmin> _serdeAdmins = new HashMap<>();
  private final Map<String, Authorizer> _authorizers = new HashMap<>();

  /**
   * Constructor for coordinator
   * @param datastreamCache Cache to maintain all the datastreams in the cluster.
   * @param config Config properties to use while creating coordinator.
   * @throws DatastreamException if coordinator creation fails.
   */
  public Coordinator(CachedDatastreamReader datastreamCache, Properties config) throws DatastreamException {
    this(datastreamCache, new CoordinatorConfig(config));
  }

  /**
   * Construtor for coordinator
   * @param datastreamCache Cache to maintain all the datastreams in the cluster.
   * @param config Coordinator config to use while creating coordinator.
   */
  public Coordinator(CachedDatastreamReader datastreamCache, CoordinatorConfig config) throws DatastreamException {
    _datastreamCache = datastreamCache;
    _config = config;
    _clusterName = _config.getCluster();
    _heartbeatPeriod = Duration.ofMillis(config.getHeartbeatPeriodMs());

    _adapter = new ZkAdapter(_config.getZkAddress(), _clusterName, _config.getDefaultTransportProviderName(),
        _config.getZkSessionTimeout(), _config.getZkConnectionTimeout(), this);

    _eventQueue = new CoordinatorEventBlockingQueue();
    _eventThread = new CoordinatorEventProcessor();
    _eventThread.setDaemon(true);

    _dynamicMetricsManager = DynamicMetricsManager.getInstance();
    _dynamicMetricsManager.registerGauge(MODULE, NUM_PAUSED_DATASTREAMS_GROUPS, () -> _pausedDatastreamsGroups.get());
    _dynamicMetricsManager.registerGauge(MODULE, IS_LEADER, () -> getIsLeader().getAsBoolean() ? 1 : 0);

    // Creating a separate thread pool for making the onAssignmentChange calls to the connector
    _assignmentChangeThreadPool = new ThreadPoolExecutor(config.getAssignmentChangeThreadPoolThreadCount(),
        config.getAssignmentChangeThreadPoolThreadCount(), 10, TimeUnit.SECONDS, new LinkedBlockingQueue<>());

    VerifiableProperties coordinatorProperties = new VerifiableProperties(_config.getConfigProperties());

    _eventProducerConfig = coordinatorProperties.getDomainProperties(EVENT_PRODUCER_CONFIG_DOMAIN);

    _cpProvider = new ZookeeperCheckpointProvider(_adapter);
    Optional.ofNullable(_cpProvider.getMetricInfos()).ifPresent(_metrics::addAll);

    _metrics.addAll(EventProducer.getMetricInfos());
  }

  /**
   * Start Coordinator (and all connectors)
   */
  public void start() {
    _log.info("Starting coordinator");
    _eventThread.start();
    _adapter.connect();

    for (String connectorType : _connectors.keySet()) {
      ConnectorInfo connectorInfo = _connectors.get(connectorType);
      ConnectorWrapper connector = connectorInfo.getConnector();

      // populate the instanceName. We only know the instance name after _adapter.connect()
      connector.setInstanceName(getInstanceName());

      // make sure connector znode exists upon instance start. This way in a brand new cluster
      // we can inspect ZooKeeper and know what connectors are created
      _adapter.ensureConnectorZNode(connector.getConnectorType());

      // call connector::start API
      connector.start(connectorInfo.getCheckpointProvider());

      _log.info("Coordinator started");
    }

    // now that instance is started, make sure it doesn't miss any assignment created during
    // the slow startup
    _eventQueue.put(CoordinatorEvent.createHandleAssignmentChangeEvent());

    // Queue up one heartbeat per period with a initial delay of 3 periods
    _executor.scheduleAtFixedRate(() -> _eventQueue.put(CoordinatorEvent.HEARTBEAT_EVENT),
        _heartbeatPeriod.toMillis() * 3, _heartbeatPeriod.toMillis(), TimeUnit.MILLISECONDS);
  }

  /**
   * Stop coordinator (and all connectors)
   */
  public void stop() {
    _log.info("Stopping coordinator");

    // Stopping event threads so that no more events are scheduled for the connector.
    while (_eventThread.isAlive()) {
      try {
        _eventThread.interrupt();
        _eventThread.join(EVENT_THREAD_JOIN_TIMEOUT);
      } catch (InterruptedException e) {
        _log.warn("Exception caught while stopping coordinator", e);
      }
    }

    // Stopping all the connectors so that they stop producing.
    for (String connectorType : _connectors.keySet()) {
      try {
        _connectors.get(connectorType).getConnector().stop();
      } catch (Exception ex) {
        _log.warn(String.format(
            "Connector stop threw an exception for connectorType %s, " + "Swallowing it and continuing shutdown.",
            connectorType), ex);
      }
    }

    // Shutdown the event producer.
    for (DatastreamTask task : _assignedDatastreamTasks.values()) {
      ((EventProducer) task.getEventProducer()).shutdown();
    }
    _adapter.disconnect();
    _log.info("Coordinator stopped");
  }

  /**
   * Notify all instances in the cluster that some datastreams get updated. We need this because currently
   * Coordinator wouldn't watch the data change within a datastream. So they won't be able to react to
   * a datastream update unless explicitly get notified.
   */
  public synchronized void broadcastDatastreamUpdate() {
    _adapter.touchAllInstanceAssignments();
  }

  public String getInstanceName() {
    return _adapter.getInstanceName();
  }

  public Collection<DatastreamTask> getDatastreamTasks() {
    return _assignedDatastreamTasks.values();
  }

  /**
   * {@inheritDoc}
   * There can only be one leader in a datastream cluster.
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
   * {@inheritDoc}
   * This method is called when a new datastream server is added or existing datastream server goes down.
   */
  @Override
  public void onLiveInstancesChange() {
    _log.info("Coordinator::onLiveInstancesChange is called");
    _eventQueue.put(CoordinatorEvent.createLeaderDoAssignmentEvent());
    _log.info("Coordinator::onLiveInstancesChange completed successfully");
  }

  /**
   * {@inheritDoc}
   * This method is called when a new datastream is created/deleted.
   */
  @Override
  public void onDatastreamAddOrDrop() {
    _log.info("Coordinator::onDatastreamAddOrDrop is called");
    // if there are new datastreams created, we need to trigger the topic creation logic
    _eventQueue.put(CoordinatorEvent.createHandleDatastreamAddOrDeleteEvent());
    _eventQueue.put(CoordinatorEvent.createLeaderDoAssignmentEvent());
    _log.info("Coordinator::onDatastreamAddOrDrop completed successfully");
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void onDatastreamUpdate() {
    _log.info("Coordinator::onDatastreamUpdate is called");
    // We need this synchronization to protect the updates on _assignedDatastreamTasks
    synchronized (_assignedDatastreamTasks) {
      // On datastream update the CachedDatastreamReader won't refresh its data, so we need to invalidate the cache
      _datastreamCache.invalidateAllCache();
      List<DatastreamGroup> datastreamGroups = _datastreamCache.getDatastreamGroups();
      // Refresh the datastream task
      _assignedDatastreamTasks.values().forEach(task -> {
        Optional<DatastreamGroup> dg =
            datastreamGroups.stream().filter(x -> x.getTaskPrefix().equals(task.getTaskPrefix())).findFirst();
        if (dg.isPresent()) {
          ((DatastreamTaskImpl) task).setDatastreams(dg.get().getDatastreams());
        } else {
          _log.warn("Can't find datastream group for task {}", task);
        }
      });
    }
    _eventQueue.put(CoordinatorEvent.createHandleDatastreamChangeEvent());
    _log.info("Coordinator::onDatastreamUpdate completed successfully");
  }

  /**
   * {@inheritDoc}
   *
   * To handle assignment change, we need to take the following steps:
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

  private void handleAssignmentChange(boolean isDatastreamUpdate) throws TimeoutException {
    long startAt = System.currentTimeMillis();

    // when there is any change to the assignment for this instance. Need to find out what is the connector
    // type of the changed assignment, and then call the corresponding callback of the connector instance
    List<String> assignment = _adapter.getInstanceAssignment(_adapter.getInstanceName());

    _log.info("START: Coordinator::handleAssignmentChange. Instance: " + _adapter.getInstanceName() + ", assignment: "
        + assignment + " isDatastreamUpdate: " + isDatastreamUpdate);

    // all datastream tasks for all connector types
    Map<String, List<DatastreamTask>> currentAssignment = new HashMap<>();
    assignment.forEach(ds -> {
      DatastreamTask task = getDatastreamTask(ds);
      if (task != null) {
        String connectorType = task.getConnectorType();
        if (!currentAssignment.containsKey(connectorType)) {
          currentAssignment.put(connectorType, new ArrayList<>());
        }
        currentAssignment.get(connectorType).add(task);
      }
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
    List<String> newConnectorList = new ArrayList<>(currentAssignment.keySet());

    List<String> deactivated = new ArrayList<>(oldConnectorList);
    deactivated.removeAll(newConnectorList);
    List<Future<Boolean>> assignmentChangeFutures = deactivated.stream()
        .map(connectorType -> dispatchAssignmentChangeIfNeeded(connectorType, new ArrayList<>(), isDatastreamUpdate))
        .filter(Objects::nonNull)
        .collect(Collectors.toList());

    // case (2) - Dispatch all the assignment changes in a separate thread
    assignmentChangeFutures.addAll(newConnectorList.stream()
        .map(connectorType -> dispatchAssignmentChangeIfNeeded(connectorType, currentAssignment.get(connectorType),
            isDatastreamUpdate))
        .filter(Objects::nonNull)
        .collect(Collectors.toList()));

    // Wait till all the futures are complete or timeout.
    Instant start = Instant.now();
    try {
      for (Future<Boolean> assignmentChangeFuture : assignmentChangeFutures) {
        if (Duration.between(start, Instant.now()).compareTo(ASSIGNMENT_TIMEOUT) > 0) {
          throw new TimeoutException("Timeout doing assignment");
        }
        try {
          assignmentChangeFuture.get(ASSIGNMENT_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
          _log.warn("onAssignmentChange call threw exception", e);
        }
      }
    } catch (TimeoutException e) {
      // if it's timeout then we will retry
      _log.warn("Timeout when doing the assignment", e);
      if (isDatastreamUpdate) {
        _eventQueue.put(CoordinatorEvent.createHandleDatastreamChangeEvent());
      } else {
        _eventQueue.put(CoordinatorEvent.createHandleAssignmentChangeEvent());
      }
      throw e;
    } catch (InterruptedException e) {
      _log.warn("onAssignmentChange call got interrupted", e);
    } finally {
      assignmentChangeFutures.forEach(future -> future.cancel(true));
    }

    // now save the current assignment
    _assignedDatastreamTasks.clear();
    _assignedDatastreamTasks.putAll(currentAssignment.values()
        .stream()
        .flatMap(Collection::stream)
        .collect(Collectors.toMap(DatastreamTask::getDatastreamTaskName, Function.identity())));

    long endAt = System.currentTimeMillis();

    _log.info(String.format("END: Coordinator::handleAssignmentChange, Duration: %d milliseconds", endAt - startAt));
    _dynamicMetricsManager.createOrUpdateMeter(MODULE, NUM_ASSIGNMENT_CHANGES, 1);
  }

  private DatastreamTask getDatastreamTask(String taskName) {
    if (_assignedDatastreamTasks.containsKey(taskName)) {
      return _assignedDatastreamTasks.get(taskName);
    } else {
      DatastreamTaskImpl task = _adapter.getAssignedDatastreamTask(_adapter.getInstanceName(), taskName);

      if (task != null) {
        DatastreamGroup dg = _datastreamCache.getDatastreamGroups()
            .stream()
            .filter(x -> x.getTaskPrefix().equals(task.getTaskPrefix()))
            .findFirst()
            .get();

        task.setDatastreams(dg.getDatastreams());
      }

      return task;
    }
  }

  private Future<Boolean> dispatchAssignmentChangeIfNeeded(String connectorType, List<DatastreamTask> assignment,
      boolean isDatastreamUpdate) {
    ConnectorInfo connectorInfo = _connectors.get(connectorType);
    ConnectorWrapper connector = connectorInfo.getConnector();

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

    if (isDatastreamUpdate || !addedTasks.isEmpty() || !removedTasks.isEmpty()) {
      // Populate the event producers before calling the connector with the list of tasks.
      addedTasks.stream().filter(t -> t.getEventProducer() == null).forEach(this::initializeTask);

      // Dispatch the onAssignmentChange to the connector in a separate thread.
      return _assignmentChangeThreadPool.submit(() -> {
        try {
          connector.onAssignmentChange(assignment);
          // Unassign tasks with producers
          removedTasks.forEach(this::uninitializeTask);
        } catch (Exception ex) {
          _log.warn(String.format("connector.onAssignmentChange for connector %s threw an exception, "
              + "Queuing up a new onAssignmentChange event for retry.", connectorType), ex);
          _eventQueue.put(CoordinatorEvent.createHandleInstanceErrorEvent(ExceptionUtils.getRootCauseMessage(ex)));
          if (isDatastreamUpdate) {
            _eventQueue.put(CoordinatorEvent.createHandleDatastreamChangeEvent());
          } else {
            _eventQueue.put(CoordinatorEvent.createHandleAssignmentChangeEvent());
          }
          return false;
        }
        return true;
      });
    }

    return null;
  }

  private void uninitializeTask(DatastreamTask t) {
    TransportProviderAdmin tpAdmin = _transportProviderAdmins.get(t.getTransportProviderName());
    tpAdmin.unassignTransportProvider(t);
    _cpProvider.unassignDatastreamTask(t);
  }

  private void initializeTask(DatastreamTask task) {
    DatastreamTaskImpl taskImpl = (DatastreamTaskImpl) task;
    assignSerdes(taskImpl);

    boolean customCheckpointing = _connectors.get(task.getConnectorType()).isCustomCheckpointing();
    TransportProviderAdmin tpAdmin = _transportProviderAdmins.get(task.getTransportProviderName());
    TransportProvider transportProvider = tpAdmin.assignTransportProvider(task);
    EventProducer producer =
        new EventProducer(task, transportProvider, _cpProvider, _eventProducerConfig, customCheckpointing);

    taskImpl.setEventProducer(producer);
    Map<Integer, String> checkpoints = producer.loadCheckpoints(task);
    taskImpl.setCheckpoints(checkpoints);
  }

  private void assignSerdes(DatastreamTaskImpl datastreamTask) {
    Datastream datastream = datastreamTask.getDatastreams().get(0);
    SerDeSet destinationSet = null;

    if (datastream.hasDestination()) {
      DatastreamDestination destination = datastream.getDestination();
      SerDe envelopeSerDe = null;
      SerDe keySerDe = null;
      SerDe valueSerDe = null;
      if (destination.hasEnvelopeSerDe() && StringUtils.isNotEmpty(destination.getEnvelopeSerDe())) {
        envelopeSerDe = _serdeAdmins.get(destination.getEnvelopeSerDe()).assignSerde(datastreamTask);
      }

      if (destination.hasKeySerDe() && StringUtils.isNotEmpty(destination.getKeySerDe())) {
        keySerDe = _serdeAdmins.get(destination.getKeySerDe()).assignSerde(datastreamTask);
      }

      if (destination.hasPayloadSerDe() && StringUtils.isNotEmpty(destination.getPayloadSerDe())) {
        valueSerDe = _serdeAdmins.get(destination.getPayloadSerDe()).assignSerde(datastreamTask);
      }

      destinationSet = new SerDeSet(keySerDe, valueSerDe, envelopeSerDe);
    }

    datastreamTask.assignSerDes(destinationSet);
  }

  protected synchronized void handleEvent(CoordinatorEvent event) {
    _log.info("START: Handle event " + event.getType() + ", Instance: " + _adapter.getInstanceName());

    try {
      switch (event.getType()) {
        case LEADER_DO_ASSIGNMENT:
          handleLeaderDoAssignment();
          break;

        case HANDLE_ASSIGNMENT_CHANGE:
          // synchronize between this and onDatastreamUpdate. See more comments there
          synchronized (_assignedDatastreamTasks) {
            handleAssignmentChange(false);
          }
          break;

        case HANDLE_DATASTREAM_CHANGE_WITH_UPDATE:
          // synchronize between this and onDatastreamUpdate. See more comments there
          synchronized (_assignedDatastreamTasks) {
            handleAssignmentChange(true);
          }
          break;

        case HANDLE_ADD_OR_DELETE_DATASTREAM:
          handleDatastreamAddOrDelete();
          break;

        case HANDLE_INSTANCE_ERROR:
          handleInstanceError((CoordinatorEvent.HandleInstanceError) event);
          break;

        case HEARTBEAT:
          handleHeartbeat();
          break;

        case LEADER_PARTITION_ASSIGNMENT:
          performPartitionAssignment(event.getDatastreamGroupName());
          break;

        default:
          String errorMessage = String.format("Unknown event type %s.", event.getType());
          ErrorLogger.logAndThrowDatastreamRuntimeException(_log, errorMessage, null);
          break;
      }
    } catch (Exception e) {
      _dynamicMetricsManager.createOrUpdateMeter(MODULE, "handleEvent-" + event.getType(), NUM_ERRORS, 1);
      _log.error("ERROR: event + " + event + " failed.", e);
    }

    _log.info("END: Handle event " + event);
  }

  // when we encounter an error, we need to persist the error message in ZooKeeper. We only persist the
  // first 10 messages. Why we put this logic in event loop instead of synchronously handle it? This
  // is because the same reason that can result in error can also result in the failure of persisting
  // the error message.
  private void handleInstanceError(CoordinatorEvent.HandleInstanceError event) {
    String msg = event.getEventData();
    _adapter.zkSaveInstanceError(msg);
  }

  /**
   * Increment a heartbeat counter as a way to report liveliness of the coordinator
   */
  private void handleHeartbeat() {
    _dynamicMetricsManager.createOrUpdateCounter(MODULE, NUM_HEARTBEATS, 1);
  }

  /**
   * Check if a datastream is either marked as deleting or its TTL has expired
   */
  private boolean isDeletingOrExpired(Datastream stream) {
    boolean isExpired = false;

    // Check TTL
    if (stream.getMetadata().containsKey(TTL_MS) && stream.getMetadata().containsKey(CREATION_MS)) {
      try {
        long ttlMs = Long.parseLong(stream.getMetadata().get(TTL_MS));
        long creationMs = Long.parseLong(stream.getMetadata().get(CREATION_MS));
        if (System.currentTimeMillis() - creationMs >= ttlMs) {
          isExpired = true;
        }
      } catch (NumberFormatException e) {
        _log.error("Ignoring TTL as some metadata is not numeric, CREATION_MS={}, TTL_MS={}",
            stream.getMetadata().get(CREATION_MS), stream.getMetadata().get(TTL_MS), e);
      }
    }

    return isExpired || stream.getStatus() == DatastreamStatus.DELETING;
  }

  /**
   * This method performs two tasks:
   * 1) initializes destination for a newly created datastream and update it in ZooKeeper
   * 2) delete an existing datastream if it is marked as deleted or its TTL has expired.
   *
   * If #2 occurs, it also invalidates the datastream cache for the next assignment.
   *
   * This means TTL is enforced only for below events:
   *  1) new leader is elected
   *  2) a new stream is added
   *  3) an existing stream is deleted
   *
   * Note that expired streams are not handled during rebalancing which is okay because
   * if there are no more streams getting created, there is no pressure to delete streams
   * either. Also, expired streams are excluded from any future task assignments.
   */
  private void handleDatastreamAddOrDelete() {
    boolean shouldRetry = false;

    // Get the list of all datastreams
    List<Datastream> allStreams = _datastreamCache.getAllDatastreams(true);

    // do nothing if there are zero datastreams
    if (allStreams.isEmpty()) {
      _log.warn("Received a new datastream event, but there were no datastreams");
      return;
    }

    for (Datastream ds : allStreams) {
      if (ds.getStatus() == DatastreamStatus.INITIALIZING) {
        try {
          if (DatastreamUtils.isConnectorManagedDestination(ds)) {
            _log.info("Connector will manage destination(s) for datastream {}, skipping destination creation.", ds);
          } else {
            createTopic(ds);
          }

          // Set the datastream status as ready for use (both producing and consumption)
          ds.setStatus(DatastreamStatus.READY);
          if (!_adapter.updateDatastream(ds)) {
            _log.warn("Failed to update datastream: {} after initializing. This datastream will not be scheduled for "
                + "producing events ", ds.getName());
            shouldRetry = true;
          }
        } catch (Exception e) {
          _log.warn("Failed to update the destination of new datastream {}", ds, e);
          shouldRetry = true;
        }
      } else if (isDeletingOrExpired(ds)) {
        _log.info("Trying to hard delete datastream {} (reason={})", ds,
            ds.getStatus() == DatastreamStatus.DELETING ? "deleting" : "expired");

        hardDeleteDatastream(ds, allStreams);
      }
    }

    if (shouldRetry) {
      _dynamicMetricsManager.createOrUpdateMeter(MODULE, "handleDatastreamAddOrDelete", NUM_RETRIES, 1);

      // If there are any failure, we will need to schedule retry if
      // there is no pending retry scheduled already.
      if (leaderDatastreamAddOrDeleteEventScheduled.compareAndSet(false, true)) {
        _log.warn("Schedule retry for handling new datastream");
        _executor.schedule(() -> {
          _eventQueue.put(CoordinatorEvent.createHandleDatastreamAddOrDeleteEvent());

          // Allow further retry scheduling
          leaderDatastreamAddOrDeleteEventScheduled.set(false);
        }, _config.getRetryIntervalMs(), TimeUnit.MILLISECONDS);
      }
    }

    _eventQueue.put(CoordinatorEvent.createLeaderDoAssignmentEvent());
  }

  private void hardDeleteDatastream(Datastream ds, List<Datastream> allStreams) {
    String taskPrefix;
    if (DatastreamUtils.containsTaskPrefix(ds)) {
      taskPrefix = DatastreamUtils.getTaskPrefix(ds);
    } else {
      taskPrefix = DatastreamTaskImpl.getTaskPrefix(ds);
    }

    Optional<Datastream> duplicateStream = allStreams.stream()
        .filter(DatastreamUtils::containsTaskPrefix)
        .filter(x -> !x.getName().equals(ds.getName()) && DatastreamUtils.getTaskPrefix(x).equals(taskPrefix))
        .findFirst();

    if (!duplicateStream.isPresent()) {
      _log.info(
          "No datastream left in the datastream group with taskPrefix {}. Deleting all tasks corresponding to the datastream.",
          taskPrefix);
      _adapter.deleteTasksWithPrefix(_connectors.keySet(), taskPrefix);
      deleteTopic(ds);
    } else {
      _log.info("Found duplicate datastream {} for the datastream to be deleted {}. Not deleting the tasks.",
          duplicateStream.get().getName(), ds.getName());
    }

    _adapter.deleteDatastream(ds.getName());
  }

  private String createTopic(Datastream datastream) throws TransportException {
    _transportProviderAdmins.get(datastream.getTransportProviderName()).createDestination(datastream);

    // For deduped datastreams, all destination-related metadata have been copied by
    // populateDatastreamDestinationFromExistingDatastream().
    if (!datastream.getMetadata().containsKey(DatastreamMetadataConstants.DESTINATION_CREATION_MS)) {
      // Set destination creation time and retention
      datastream.getMetadata()
          .put(DatastreamMetadataConstants.DESTINATION_CREATION_MS, String.valueOf(Instant.now().toEpochMilli()));

      try {
        Duration retention = _transportProviderAdmins.get(datastream.getTransportProviderName()).getRetention(datastream);
        if (retention != null) {
          datastream.getMetadata()
              .put(DatastreamMetadataConstants.DESTINATION_RETENTION_MS, String.valueOf(retention.toMillis()));
        }
      } catch (UnsupportedOperationException e) {
        _log.warn("Transport doesn't support mechanism to get retention, Unable to populate retention in datastream", e);
      }
    }

    return datastream.getDestination().getConnectionString();
  }

  private void deleteTopic(Datastream datastream) {
    try {
      if (DatastreamUtils.isUserManagedDestination(datastream)) {
        _log.info("BYOT(bring your own topic), topic will not be deleted");
      } else if (DatastreamUtils.isConnectorManagedDestination(datastream)) {
        _log.info("Datastream contains connector-managed destinations, topic will not be deleted");
      } else {
        _transportProviderAdmins.get(datastream.getTransportProviderName()).dropDestination(datastream);
      }
    } catch (Exception e) {
      _log.error("Runtime Exception while delete topic", e);
    }
  }

  private List<DatastreamGroup> fetchDatastreamGroups() {
    // Get all streams that are assignable. Assignable datastreams are the ones:
    //  1) has a valid destination
    //  2) status is READY or PAUSED, STOPPED or other datastream status will NOT get assigned
    //  3) TTL has not expired
    // Note: We do not need to flush the cache, because the datastreams should have been read as part of the
    //       handleDatastreamAddOrDelete event (that should occur before handleLeaderDoAssignment)
    List<Datastream> allStreams = _datastreamCache.getAllDatastreams(false)
        .stream()
        .filter(datastream -> datastream.hasStatus() && (datastream.getStatus() == DatastreamStatus.READY
            || datastream.getStatus() == DatastreamStatus.PAUSED) && hasValidDestination(datastream)
            && !isDeletingOrExpired(datastream))
        .collect(Collectors.toList());

    Set<Datastream> invalidDatastreams =
        allStreams.stream().filter(s -> !DatastreamUtils.containsTaskPrefix(s)).collect(Collectors.toSet());
    if (!invalidDatastreams.isEmpty()) {
      _log.error("Datastreams {} are ignored during assignment because they didn't contain task prefixes",
          invalidDatastreams);
    }

    // Process only the streams that contains the taskPrefix.
    Map<String, List<Datastream>> streamsByTaskPrefix = allStreams.stream()
        .filter(s -> !invalidDatastreams.contains(s))
        .collect(Collectors.groupingBy(DatastreamUtils::getTaskPrefix, Collectors.toList()));

    return streamsByTaskPrefix.keySet()
        .stream()
        .map(x -> new DatastreamGroup(streamsByTaskPrefix.get(x)))
        .collect(Collectors.toList());
  }

  private void handleLeaderDoAssignment() {
    boolean succeeded = true;
    List<String> liveInstances = Collections.emptyList();
    Map<String, Set<DatastreamTask>> previousAssignmentByInstance = Collections.emptyMap();
    Map<String, List<DatastreamTask>> newAssignmentsByInstance = Collections.emptyMap();

    try {
      List<DatastreamGroup> datastreamGroups = fetchDatastreamGroups();

      onDatastreamChange(datastreamGroups);

      _log.debug("handleLeaderDoAssignment: final datastreams for task assignment: {}", datastreamGroups);

      // get all current live instances
      liveInstances = _adapter.getLiveInstances();

      // Map between instance to tasks assigned to the instance.
      previousAssignmentByInstance = _adapter.getAllAssignedDatastreamTasks();

      // Map between Instance and the tasks
      newAssignmentsByInstance = performAssignment(liveInstances, previousAssignmentByInstance, datastreamGroups);

      // persist the assigned result to ZooKeeper. This means we will need to compare with the current
      // assignment and do remove and add zNodes accordingly. In the case of ZooKeeper failure (when
      // it failed to create or delete zNodes), we will do our best to continue the current process
      // and schedule a retry. The retry should be able to diff the remaining ZooKeeper work
      _adapter.updateAllAssignments(newAssignmentsByInstance);
    } catch (RuntimeException e) {
      _log.error("handleLeaderDoAssignment: runtime exception.", e);
      succeeded = false;
    }

    _log.info("handleLeaderDoAssignment: completed ");
    _log.debug("handleLeaderDoAssignment: new assignment: " + newAssignmentsByInstance);

    // clean up tasks under dead instances if everything went well
    if (succeeded) {
      List<String> instances = new ArrayList<>(liveInstances);
      instances.add(PAUSED_INSTANCE);
      _adapter.cleanupDeadInstanceAssignments(instances);
      _adapter.cleanupOldUnusedTasks(previousAssignmentByInstance, newAssignmentsByInstance);
      _dynamicMetricsManager.createOrUpdateMeter(MODULE, NUM_REBALANCES, 1);
    }

    // schedule retry if failure
    if (!succeeded && !leaderDoAssignmentScheduled.get()) {
      _log.info("Schedule retry for leader assigning tasks");
      _dynamicMetricsManager.createOrUpdateMeter(MODULE, "handleLeaderDoAssignment", NUM_RETRIES, 1);
      leaderDoAssignmentScheduled.set(true);
      _executor.schedule(() -> {
        _eventQueue.put(CoordinatorEvent.createLeaderDoAssignmentEvent());
        leaderDoAssignmentScheduled.set(false);
      }, _config.getRetryIntervalMs(), TimeUnit.MILLISECONDS);
    }
  }

  private void performPartitionAssignment(Optional<String> datastreamGroupName) {
    if (!datastreamGroupName.isPresent()) {
      _log.error("Datastream group is not found when performing partition assignment");
      return;
    }

    boolean succeeded = false;
    Map<String, Set<DatastreamTask>> previousAssignmentByInstance = new HashMap<>();
    Map<String, List<DatastreamTask>> newAssignmentsByInstance = new HashMap<>();
    try {
      previousAssignmentByInstance = _adapter.getAllAssignedDatastreamTasks();
      Map<String, Set<DatastreamTask>> assignmentByInstance = new HashMap<>(previousAssignmentByInstance);

      // retrieve the datastreamGroups for validation
      List<String> datastreamGroups = fetchDatastreamGroups().stream().map(DatastreamGroup::getTaskPrefix)
          .collect(Collectors.toList());

      StickyPartitionAssignmentStrategy partitionAssignmentStrategy = new StickyPartitionAssignmentStrategy();

      for (String connectorType : _connectors.keySet()) {
        Connector connectorInstance = _connectors.get(connectorType).getConnector().getConnectorInstance();
        Map<String, Optional<DatastreamPartitionsMetadata>> datastreamPartitions =
            connectorInstance.getDatastreamPartitions();

        //if the datastreamGroupName is specified, we process for that datastream only
        datastreamGroups.retainAll(datastreamPartitions.keySet());
        datastreamGroupName.ifPresent(dg -> datastreamGroups.retainAll(ImmutableList.of(dg)));
        for (String dgName : datastreamGroups) {
          DatastreamPartitionsMetadata subscribes = connectorInstance.getDatastreamPartitions().get(dgName)
              .orElseThrow(() -> new DatastreamTransientException("Subscribed partition is not ready yet for datastream " + dgName));
          assignmentByInstance = partitionAssignmentStrategy.assignPartitions(assignmentByInstance, subscribes);
        }
    }

      _log.info("Partition assignment completed: datastreamGroup, assignment {} ", assignmentByInstance);
      for (String key : assignmentByInstance.keySet()) {
        newAssignmentsByInstance.put(key, new ArrayList<>(assignmentByInstance.get(key)));
      }
      _adapter.updateAllAssignments(newAssignmentsByInstance);

      succeeded = true;
    } catch (Exception ex) {
      _log.info("Partition assignment failed, Exception: ", ex);
    }
    // schedule retry if failure
    if (succeeded) {
      _adapter.cleanupOldUnusedTasks(previousAssignmentByInstance, newAssignmentsByInstance);
      getMaxPartitionCountInTask(newAssignmentsByInstance);
      _dynamicMetricsManager.createOrUpdateMeter(MODULE, NUM_PARTITION_ASSIGNMENTS, 1);
    } else if (!leaderPartitionAssignmentScheduled.get()) {
      _log.info("Schedule retry for leader assigning tasks");
      _dynamicMetricsManager.createOrUpdateMeter(MODULE, "handleLeaderPartitionAssignment", NUM_RETRIES, 1);
      leaderPartitionAssignmentScheduled.set(true);
      _executor.schedule(() -> {
        _eventQueue.put(CoordinatorEvent.createLeaderPartitionAssignmentEvent(datastreamGroupName.get()));
        leaderPartitionAssignmentScheduled.set(false);
      }, _config.getRetryIntervalMs(), TimeUnit.MILLISECONDS);
    }
  }

  private void getMaxPartitionCountInTask(Map<String, List<DatastreamTask>> assignments) {
    long maxPartitionCount = 0;
    for (List<DatastreamTask> tasks : assignments.values()) {
      maxPartitionCount = Math.max(maxPartitionCount,
          tasks.stream().map(DatastreamTask::getPartitionsV2).map(List::size).mapToInt(v -> v).max().orElse(0));
    }
    _dynamicMetricsManager.createOrUpdateMeter(MODULE, MAX_PARTITION_COUNT_IN_TASK, maxPartitionCount);
  }


  private void onDatastreamChange(List<DatastreamGroup> datastreamGroups) {
    //We need to perform handleDatastream only active datastream for partition listening
    List<DatastreamGroup> activeDataStreams = datastreamGroups.stream().filter(dg -> !dg.isPaused()).collect(Collectors.toList());

    for (String connectorType : _connectors.keySet()) {
      ConnectorWrapper connectorWrapper = _connectors.get(connectorType).getConnector();

      List<DatastreamGroup> datastreamsPerConnectorType = activeDataStreams.stream()
          .filter(x -> x.getConnectorName().equals(connectorType))
          .collect(Collectors.toList());

      connectorWrapper.getConnectorInstance().handleDatastream(datastreamsPerConnectorType);
    }
  }

  private Map<String, List<DatastreamTask>> performAssignment(List<String> liveInstances,
      Map<String, Set<DatastreamTask>> previousAssignmentByInstance, List<DatastreamGroup> datastreamGroups) {
    Map<String, List<DatastreamTask>> newAssignmentsByInstance = new HashMap<>();

    _log.info("handleLeaderDoAssignment: start");
    _log.debug("handleLeaderDoAssignment: assignment before re-balancing: " + previousAssignmentByInstance);

    Set<DatastreamGroup> pausedDatastreamGroups =
        datastreamGroups.stream().filter(DatastreamGroup::isPaused).collect(Collectors.toSet());

    _pausedDatastreamsGroups.set(pausedDatastreamGroups.size());

    // If a datastream group is paused, park tasks with the virtual PausedInstance.
    List<DatastreamTask> pausedTasks = pausedTasks(pausedDatastreamGroups, previousAssignmentByInstance);
    if (!pausedTasks.isEmpty()) {
      _log.info("Paused Task count:" + pausedTasks.size() + "; Task list: " + pausedTasks);
    }
    newAssignmentsByInstance.put(PAUSED_INSTANCE, pausedTasks);

    for (String connectorType : _connectors.keySet()) {
      AssignmentStrategy strategy = _connectors.get(connectorType).getAssignmentStrategy();
      List<DatastreamGroup> datastreamsPerConnectorType = datastreamGroups.stream()
          .filter(x -> x.getConnectorName().equals(connectorType))
          .filter(g -> !(pausedDatastreamGroups.contains(g)))
          .collect(Collectors.toList());

      // Get the list of tasks per instance for the given connector type
      // We need to call assign even if the number of datastreams are empty, This is to make sure that
      // the assignments get cleaned up for the deleted datastreams.
      Map<String, Set<DatastreamTask>> tasksByConnectorAndInstance =
          strategy.assign(datastreamsPerConnectorType, liveInstances, previousAssignmentByInstance);

      for (String instance : tasksByConnectorAndInstance.keySet()) {
        newAssignmentsByInstance.computeIfAbsent(instance, (x) -> new ArrayList<>());

        // Add the tasks for this connector type to the instance
        tasksByConnectorAndInstance.get(instance).forEach(task -> {
          // Each task must have a valid zkAdapter
          ((DatastreamTaskImpl) task).setZkAdapter(_adapter);
          if (task.getStatus() != null && DatastreamTaskStatus.Code.PAUSED.equals(task.getStatus().getCode())) {
            // Removed the Paused Status.
            task.setStatus(DatastreamTaskStatus.ok());
          }
          newAssignmentsByInstance.get(instance).add(task);
        });
      }
    }

    return newAssignmentsByInstance;
  }

  /**
   * Get tasks assigned to paused groups
   */
  private List<DatastreamTask> pausedTasks(Collection<DatastreamGroup> pausedDatastreamGroups,
      Map<String, Set<DatastreamTask>> currentlyAssignedDatastream) {
    List<DatastreamTask> currentlyAssignedDatastreamTasks =
        currentlyAssignedDatastream.values().stream().flatMap(Collection::stream).collect(Collectors.toList());

    List<DatastreamTask> pausedTasks = new ArrayList<>();
    for (DatastreamGroup dg : pausedDatastreamGroups) {
      currentlyAssignedDatastreamTasks.stream().filter(dg::belongsTo).forEach(task -> {
        if (task.getStatus() == null || DatastreamTaskStatus.Code.OK.equals(task.getStatus().getCode())) {
          // Set task status to Paused.
          task.setStatus(DatastreamTaskStatus.paused());
        }
        pausedTasks.add(task);
      });
    }

    return pausedTasks;
  }

  /**
   * Add a connector to the coordinator. A coordinator can handle multiples type of connectors, but only one
   * connector per connector type.
   *
   * @param connectorName of the connector.
   * @param connector a connector that implements the Connector interface
   * @param strategy the assignment strategy deciding how to distribute datastream tasks among instances
   * @param customCheckpointing whether connector uses custom checkpointing. if the custom checkpointing is set to true
   *                            Coordinator will not perform checkpointing to ZooKeeper.
   * @param deduper the deduper used by connector
   * @param authorizerName name of the authorizer configured by connector
   * @param enablePartitionAssignment enable partition assignment for this connector
   *
   */
  public void addConnector(String connectorName, Connector connector, AssignmentStrategy strategy,
      boolean customCheckpointing, DatastreamDeduper deduper, String authorizerName, boolean enablePartitionAssignment) {
    Validate.notNull(strategy, "strategy cannot be null");
    Validate.notEmpty(connectorName, "connectorName cannot be empty");
    Validate.notNull(connector, "Connector cannot be null");

    _log.info("Add new connector of type {}, strategy {} with custom checkpointing {} to coordinator", connectorName,
        strategy.getClass().getTypeName(), customCheckpointing);

    if (_connectors.containsKey(connectorName)) {
      String err = "A connector of type " + connectorName + " already exists.";
      _log.error(err);
      throw new IllegalArgumentException(err);
    }

    Optional<List<BrooklinMetricInfo>> connectorMetrics = Optional.ofNullable(connector.getMetricInfos());
    connectorMetrics.ifPresent(_metrics::addAll);


    if (enablePartitionAssignment) {
      connector.onPartitionChange(datastreamGroup ->
        _eventQueue.put(CoordinatorEvent.createLeaderPartitionAssignmentEvent(datastreamGroup.getTaskPrefix()))
      );
    }

    ConnectorInfo connectorInfo =
        new ConnectorInfo(connectorName, connector, strategy, customCheckpointing, _cpProvider, deduper, authorizerName);
    _connectors.put(connectorName, connectorInfo);

    // Register common connector metrics
    // Use connector name for the metrics, as there can be multiple connectors specified in the config that use
    // same connector class.
    _dynamicMetricsManager.registerGauge(connectorName, NUM_DATASTREAMS,
        () -> connectorInfo.getConnector().getNumDatastreams());
    _dynamicMetricsManager.registerGauge(connectorName, NUM_DATASTREAM_TASKS,
        () -> connectorInfo.getConnector().getNumDatastreamTasks());

    _metrics.add(new BrooklinGaugeInfo(MetricRegistry.name(connectorName, NUM_DATASTREAMS)));
    _metrics.add(new BrooklinGaugeInfo(MetricRegistry.name(connectorName, NUM_DATASTREAM_TASKS)));
  }

  /**
   * Add a connector to the coordinator with partitionAssignment to be disabled
   */
  public void addConnector(String connectorName, Connector connector, AssignmentStrategy strategy,
      boolean customCheckpointing, DatastreamDeduper deduper, String authorizerName) {
    addConnector(connectorName, connector, strategy, customCheckpointing, deduper, authorizerName, false);
  }

  /**
   * Validate updates to given datastreams
   * @param datastreams List of datastreams whose updates are validated.
   * @throws DatastreamValidationException if any update is invalid.
   */
  public void validateDatastreamsUpdate(List<Datastream> datastreams) throws DatastreamValidationException {
    _log.info("About to validate datastreams update: " + datastreams);
    try {
      // DatastreamResources checks ensure we dont have more than one connector type in the updated datastream list
      String connectorName = datastreams.get(0).getConnectorName();
      ConnectorInfo connectorInfo = _connectors.get(connectorName);
      if (connectorInfo == null) {
        throw new DatastreamValidationException("Invalid connector: " + connectorName);
      }

      connectorInfo.getConnector()
          .validateUpdateDatastreams(datastreams, _datastreamCache.getAllDatastreams()
              .stream()
              .filter(d -> d.getConnectorName().equals(connectorName))
              .collect(Collectors.toList()));
    } catch (Exception e) {
      _dynamicMetricsManager.createOrUpdateMeter(MODULE, "validateDatastreamsUpdate", NUM_ERRORS, 1);
      throw e;
    }
  }

  /**
   * Checks if given datastream update type is supported by connector for given datastream.
   * @param datastream datastream to check against
   * @param updateType - Type of datastream update to validate
   */
  public void isDatastreamUpdateTypeSupported(Datastream datastream, DatastreamConstants.UpdateType updateType)
      throws DatastreamValidationException {
    _log.info("About to validate datastream update type {} for datastream {}", updateType, datastream);
    try {
      String connectorName = datastream.getConnectorName();
      ConnectorInfo connectorInfo = _connectors.get(connectorName);
      if (connectorInfo == null) {
        throw new DatastreamValidationException("Invalid connector: " + datastream.getConnectorName());
      }

      if (!connectorInfo.getConnector().isDatastreamUpdateTypeSupported(datastream, updateType)) {
        throw new DatastreamValidationException(
            String.format("Datastream update of type : %s for datastream: %s is not supported by connector: %s",
                updateType, datastream.getName(), connectorName));
      }
    } catch (Exception e) {
      _dynamicMetricsManager.createOrUpdateMeter(MODULE, "isDatastreamUpdateTypeSupported", NUM_ERRORS, 1);
      throw e;
    }
  }

  /**
   * Initializes the datastream. Datastream management service will call this before writing the
   * Datastream into ZooKeeper. This method should ensure that the source has sufficient details.
   * @param datastream datastream for validation
   */
  public void initializeDatastream(Datastream datastream) throws DatastreamValidationException {
    datastream.setStatus(DatastreamStatus.INITIALIZING);
    String connectorName = datastream.getConnectorName();
    ConnectorInfo connectorInfo = _connectors.get(connectorName);
    if (connectorInfo == null) {
      String errorMessage = "Invalid connector: " + connectorName;
      _log.error(errorMessage);
      throw new DatastreamValidationException(errorMessage);
    }

    ConnectorWrapper connector = connectorInfo.getConnector();
    DatastreamDeduper deduper = connectorInfo.getDatastreamDeduper();

    // Changing a non-flush cache version to flush version to avoid errors in deduping datastreams which
    // should be deduped, but fail to due to being created back to back and ZK client not syncing with master
    List<Datastream> allDatastreams = _datastreamCache.getAllDatastreams(true)
        .stream()
        .filter(d -> d.getConnectorName().equals(connectorName))
        .collect(Collectors.toList());

    // If datastream of name already exists return error
    if (allDatastreams.stream().anyMatch(x -> x.getName().equals(datastream.getName()))) {
      String errMsg = String.format("Datastream with name %s already exists", datastream.getName());
      _log.error(errMsg);
      throw new DatastreamAlreadyExistsException(errMsg);
    }

    if (!StringUtils.isEmpty(_config.getDefaultTransportProviderName())) {
      if (!datastream.hasTransportProviderName() || StringUtils.isEmpty(datastream.getTransportProviderName())) {
        datastream.setTransportProviderName(_config.getDefaultTransportProviderName());
      }
    }

    try {
      if (connectorInfo.getAuthorizerName().isPresent()) {
        Authorizer authz = _authorizers.getOrDefault(connectorInfo.getAuthorizerName().get(), null);

        if (authz == null) {
          String errMsg = String.format("No authorizer '%s' was configured", connectorInfo.getAuthorizerName().get());
          _log.error(errMsg);
          throw new DatastreamValidationException(errMsg);
        }

        // Security principals are passed in through OWNER metadata
        // DatastreamResources has validated OWNER key is present
        String principal = datastream.getMetadata().get(DatastreamMetadataConstants.OWNER_KEY);

        // CREATE is already verified through the SSL layer of the HTTP framework (optional)
        // READ is the operation for datastream source-level authorization
        if (!authz.authorize(datastream, Authorizer.Operation.READ, () -> principal)) {
          String errMsg =
              String.format("Consumer %s has not been approved for %s over %s", principal, datastream.getSource(),
                  datastream.getTransportProviderName());
          _log.warn(errMsg);
          throw new AuthorizationException(errMsg);
        }
      }

      connector.initializeDatastream(datastream, allDatastreams);
      initializeDatastreamDestination(connector, datastream, deduper, allDatastreams);
      connector.postDatastreamInitialize(datastream, allDatastreams);
    } catch (Exception e) {
      _dynamicMetricsManager.createOrUpdateMeter(MODULE, "initializeDatastream", NUM_ERRORS, 1);
      throw e;
    }

    datastream.getMetadata().putIfAbsent(CREATION_MS, String.valueOf(Instant.now().toEpochMilli()));
  }

  private void initializeDatastreamDestination(ConnectorWrapper connector, Datastream datastream,
      DatastreamDeduper deduper, List<Datastream> allDatastreams) throws DatastreamValidationException {
    Optional<Datastream> existingDatastream = Optional.empty();

    // Dedupe datastream only when its destination is not populated and allows reuse
    if (!hasValidDestination(datastream) && isReuseAllowed(datastream)) {
      existingDatastream = deduper.findExistingDatastream(datastream, allDatastreams);
    }

    // For a BYOT datastream, check that the destination is not already in use by other streams
    if (DatastreamUtils.isUserManagedDestination(datastream)) {
      List<Datastream> sameDestinationDatastreams = allDatastreams.stream()
          .filter(
              ds -> ds.getDestination().getConnectionString().equals(datastream.getDestination().getConnectionString()))
          .collect(Collectors.toList());
      if (!sameDestinationDatastreams.isEmpty()) {
        String datastreamNames = sameDestinationDatastreams.stream()
            .map(Datastream::getName)
            .collect(Collectors.joining(", "));

        String errMsg = String.format("Cannot create a BYOT datastream where the destination is being used by other datastream(s): %s",
            datastreamNames);
        _log.error(errMsg);
        throw new DatastreamValidationException(errMsg);
      }
    }

    if (existingDatastream.isPresent()) {
      populateDatastreamDestinationFromExistingDatastream(datastream, existingDatastream.get());
    } else {
      if (!_transportProviderAdmins.containsKey(datastream.getTransportProviderName())) {
        throw new DatastreamValidationException(
            String.format("Transport provider \"%s\" is undefined", datastream.getTransportProviderName()));
      }
      String destinationName = connector.getDestinationName(datastream);
      _transportProviderAdmins.get(datastream.getTransportProviderName())
          .initializeDestinationForDatastream(datastream, destinationName);
      // Populate the task prefix if it is not already present.
      if (!datastream.getMetadata().containsKey(DatastreamMetadataConstants.TASK_PREFIX)) {
        datastream.getMetadata()
            .put(DatastreamMetadataConstants.TASK_PREFIX, DatastreamTaskImpl.getTaskPrefix(datastream));
      }

      _log.info("Datastream {} has an unique source or topicReuse is set to true, Assigning a new destination {}",
          datastream.getName(), datastream.getDestination());
    }
  }

  private void populateDatastreamDestinationFromExistingDatastream(Datastream datastream, Datastream existingStream) {
    DatastreamDestination destination = existingStream.getDestination();
    datastream.setDestination(destination);

    // Copy destination-related metadata
    existingStream.getMetadata().entrySet().stream()
        .filter(e -> e.getKey().startsWith(SYSTEM_DESTINATION_PREFIX))
        .forEach(e -> datastream.getMetadata().put(e.getKey(), e.getValue()));

    // If the existing datastream group is paused, also pause this datastream.
    // This is to avoid the creation of a datastream to RESUME event production.
    if (existingStream.getStatus().equals(DatastreamStatus.PAUSED)) {
      datastream.setStatus(DatastreamStatus.PAUSED);
    }

    datastream.getMetadata()
        .put(DatastreamMetadataConstants.TASK_PREFIX,
            existingStream.getMetadata().get(DatastreamMetadataConstants.TASK_PREFIX));
  }

  @Override
  public List<BrooklinMetricInfo> getMetricInfos() {
    _metrics.add(new BrooklinMeterInfo(buildMetricName(MODULE, NUM_REBALANCES)));
    _metrics.add(new BrooklinMeterInfo(buildMetricName(MODULE, NUM_ASSIGNMENT_CHANGES)));
    _metrics.add(new BrooklinMeterInfo(buildMetricName(MODULE, NUM_PARTITION_ASSIGNMENTS)));
    _metrics.add(new BrooklinMeterInfo(buildMetricName(MODULE, NUM_PARTITION_MOVEMENTS)));
    _metrics.add(new BrooklinMeterInfo(buildMetricName(MODULE, MAX_PARTITION_COUNT_IN_TASK)));
    _metrics.add(new BrooklinMeterInfo(getDynamicMetricPrefixRegex(MODULE) + NUM_ERRORS));
    _metrics.add(new BrooklinMeterInfo(getDynamicMetricPrefixRegex(MODULE) + NUM_RETRIES));
    _metrics.add(new BrooklinCounterInfo(buildMetricName(MODULE, NUM_HEARTBEATS)));
    _metrics.add(new BrooklinGaugeInfo(buildMetricName(MODULE, NUM_PAUSED_DATASTREAMS_GROUPS)));
    _metrics.add(new BrooklinGaugeInfo(buildMetricName(MODULE, IS_LEADER)));

    return Collections.unmodifiableList(_metrics);
  }

  /**
   * Get the datastream clusterName
   */
  public String getClusterName() {
    return _clusterName;
  }

  /**
   * Add a transport provider that the coordinator can assign to datastreams it creates.
   * @param transportProviderName Name of transport provider.
   * @param admin Instance of TransportProviderAdmin that the coordinator can assign.
   */
  public void addTransportProvider(String transportProviderName, TransportProviderAdmin admin) {
    _transportProviderAdmins.put(transportProviderName, admin);

    Optional<List<BrooklinMetricInfo>> transportProviderMetrics = Optional.ofNullable(admin.getMetricInfos());
    transportProviderMetrics.ifPresent(_metrics::addAll);
  }

  /**
   * Add a Serde that the coordinator can assign to datastreams it creates.
   * @param serdeName Name of Serde.
   * @param admin Instance of SerdeAdmin that the coordinator can assign.
   */
  public void addSerde(String serdeName, SerdeAdmin admin) {
    _serdeAdmins.put(serdeName, admin);
  }

  /**
   * Get a boolean supplier which can be queried to check if the current
   * Coordinator instance is a leader in the Brooklin server cluster. This
   * allows other part of the server to perform cluster level operations only
   * on the leader.
   */
  public BooleanSupplier getIsLeader() {
    return _adapter::isLeader;
  }

  /**
   * Set an authorizer implementation to enforce ACL on datastream CRUD operations.
   */
  public void addAuthorizer(String name, Authorizer authorizer) {
    Validate.notNull(authorizer, "null authorizer");
    if (_authorizers.containsKey(name)) {
      _log.warn("Registering duplicate authorizer with name={}, auth={}", name, authorizer);
    }
    _authorizers.put(name, authorizer);
    if (authorizer instanceof MetricsAware) {
      Optional.ofNullable(((MetricsAware) authorizer).getMetricInfos()).ifPresent(_metrics::addAll);
    }
  }

  // helper method for logging
  private String printAssignmentByType(Map<String, List<DatastreamTask>> assignment) {
    StringBuilder sb = new StringBuilder();
    sb.append("Current assignment for instance: ").append(getInstanceName()).append(":\n");
    for (Map.Entry<String, List<DatastreamTask>> entry : assignment.entrySet()) {
      sb.append(entry.getKey()).append(": ").append(entry.getValue()).append("\n");
    }
    // remove the final "\n"
    String result = sb.toString();
    return result.substring(0, result.length() - 1);
  }

  /**
   * Get connector by name
   * @param name Name of the connector.
   * @return Instance of the connector (if present), null otherwise.
   */
  public Connector getConnector(String name) {
    if (!_connectors.containsKey(name)) {
      return null;
    }
    return _connectors.get(name).getConnector().getConnectorInstance();
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
}
