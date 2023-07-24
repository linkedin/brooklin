/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.server;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.BooleanSupplier;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.Validate;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.MetricRegistry;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import com.linkedin.datastream.common.Datastream;
import com.linkedin.datastream.common.DatastreamAlreadyExistsException;
import com.linkedin.datastream.common.DatastreamConstants;
import com.linkedin.datastream.common.DatastreamDestination;
import com.linkedin.datastream.common.DatastreamException;
import com.linkedin.datastream.common.DatastreamMetadataConstants;
import com.linkedin.datastream.common.DatastreamRuntimeException;
import com.linkedin.datastream.common.DatastreamStatus;
import com.linkedin.datastream.common.DatastreamTransientException;
import com.linkedin.datastream.common.DatastreamUtils;
import com.linkedin.datastream.common.ErrorLogger;
import com.linkedin.datastream.common.PollUtils;
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
import com.linkedin.datastream.server.api.transport.TransportProvider;
import com.linkedin.datastream.server.api.transport.TransportProviderAdmin;
import com.linkedin.datastream.server.providers.CheckpointProvider;
import com.linkedin.datastream.server.providers.ZookeeperCheckpointProvider;
import com.linkedin.datastream.server.zk.ZkAdapter;

import static com.linkedin.datastream.common.DatastreamMetadataConstants.CREATION_MS;
import static com.linkedin.datastream.common.DatastreamMetadataConstants.SYSTEM_DESTINATION_PREFIX;
import static com.linkedin.datastream.common.DatastreamMetadataConstants.THROUGHPUT_VIOLATING_TOPICS;
import static com.linkedin.datastream.common.DatastreamMetadataConstants.TTL_MS;
import static com.linkedin.datastream.common.DatastreamUtils.hasValidDestination;
import static com.linkedin.datastream.common.DatastreamUtils.isReuseAllowed;
import static com.linkedin.datastream.server.CoordinatorEvent.EventType;
import static com.linkedin.datastream.server.CoordinatorEvent.EventType.HANDLE_ASSIGNMENT_CHANGE;
import static com.linkedin.datastream.server.CoordinatorEvent.EventType.HANDLE_DATASTREAM_CHANGE_WITH_UPDATE;


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
 * │              │       │ ┌──────────┐  ┌────────────────┐        │    │                 │
 * │              │       │ |          |──▶ onNewSession   │        │    |                 │
 * │              │       │ │          │  └────────────────┘        │    │                 │
 * │              │       │ |          |  ┌────────────────┐        │    │                 │
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
 * │              │       │ |          |  ┌────────────────────┐    │    │                 │
 * │              │       │ |          |──▶ onSessionExpired   ├────┼────▶                 │
 * └──────────────┘       │ └──────────┘  └────────────────────┘    │    │                 │
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

  private static final long EVENT_THREAD_LONG_JOIN_TIMEOUT = 90000L;
  private static final long EVENT_THREAD_SHORT_JOIN_TIMEOUT = 3000L;
  // how long should the leader wait between consecutive polls to zookeeper to confirm that the datastreams have stopped
  private static final long STOP_PROPAGATION_RETRY_MS = 5000L;
  // how many threads will the token claims executor use for assignment tokens feature. There's a risk that this will get
  // exhausted when there are more concurrent stop requests than threads in the thread pool
  private static final int TOKEN_CLAIM_THREAD_POOL_SIZE = 16;

  private static final Duration ASSIGNMENT_TIMEOUT = Duration.ofSeconds(90);

  private static final AtomicLong PAUSED_DATASTREAMS_GROUPS = new AtomicLong(0L);

  private static final AtomicLong MAX_PARTITION_COUNT = new AtomicLong(0L);

  private final CachedDatastreamReader _datastreamCache;
  private final Properties _eventProducerConfig;
  private final CheckpointProvider _cpProvider;
  private final Map<String, TransportProviderAdmin> _transportProviderAdmins = new HashMap<>();
  private final CoordinatorEventBlockingQueue _eventQueue;
  private final CoordinatorMetrics _metrics;
  private final Map<String, ExecutorService> _assignmentChangeThreadPool = new ConcurrentHashMap<>();
  private final String _clusterName;
  private final CoordinatorConfig _config;
  private final ZkAdapter _adapter;

  // mapping from connector type to connector Info instance
  private final Map<String, ConnectorInfo> _connectors = new HashMap<>();

  // Currently assigned datastream tasks by taskName
  private final Map<String, DatastreamTask> _assignedDatastreamTasks = new ConcurrentHashMap<>();
  // Maintain a count of onAssignmentChange retries in case of task initialization/submission failure.
  // If count reaches maximum threshold do not queue the event further. Reset the count if event raised
  // from any other path.
  private AtomicInteger _assignmentRetryCount = new AtomicInteger(0);

  // One coordinator heartbeat per minute, heartbeat helps detect dead/live-lock
  // where no events can be handled if coordinator locks up. This can happen because
  // handleEvent is synchronized and downstream code can misbehave.
  private final Duration _heartbeatPeriod;

  private final Logger _log = LoggerFactory.getLogger(Coordinator.class.getName());

  // make sure the scheduled retries are not duplicated
  private final AtomicBoolean _leaderDatastreamAddOrDeleteEventScheduled = new AtomicBoolean(false);

  // make sure the scheduled retries are not duplicated
  private final AtomicBoolean _leaderDoAssignmentScheduled = new AtomicBoolean(false);

  private final Map<String, SerdeAdmin> _serdeAdmins = new HashMap<>();
  private final Map<String, Authorizer> _authorizers = new HashMap<>();
  private volatile boolean _shutdown = false;

  private CoordinatorEventProcessor _eventThread;
  private ScheduledExecutorService _scheduledExecutor;
  private ExecutorService _tokenClaimExecutor;
  private Future<?> _leaderDatastreamAddOrDeleteEventScheduledFuture = null;
  private Future<?> _leaderDoAssignmentScheduledFuture = null;
  private volatile boolean _zkSessionExpired = false;

  // Cache all the throughput violating topics per datastream
  private final Map<String, Set<String>> _throughputViolatingTopicsMap = new HashMap<>();
  private final Function<DatastreamTask, Set<String>> _throughputViolatingTopicsProvider;

  // As the _throughputViolatingTopicsMap may be updated by watcher thread and read by multiple task threads concurrently.
  private final ReadWriteLock _throughputViolatingTopicsMapReadWriteLock = new ReentrantReadWriteLock();
  private final Lock _throughputViolatingTopicsMapWriteLock = _throughputViolatingTopicsMapReadWriteLock.writeLock();
  private final Lock _throughputViolatingTopicsMapReadLock = _throughputViolatingTopicsMapReadWriteLock.readLock();

  /**
   * Constructor for coordinator
   * @param datastreamCache Cache to maintain all the datastreams in the cluster.
   * @param config Config properties to use while creating coordinator.
   */
  public Coordinator(CachedDatastreamReader datastreamCache, Properties config) {
    this(datastreamCache, new CoordinatorConfig(config));
  }

  /**
   * Construtor for coordinator
   * @param datastreamCache Cache to maintain all the datastreams in the cluster.
   * @param config Coordinator config to use while creating coordinator.
   */
  public Coordinator(CachedDatastreamReader datastreamCache, CoordinatorConfig config) {
    _datastreamCache = datastreamCache;
    _config = config;
    _clusterName = _config.getCluster();
    _heartbeatPeriod = Duration.ofMillis(config.getHeartbeatPeriodMs());

    _adapter = createZkAdapter();
    _eventQueue = new CoordinatorEventBlockingQueue(Coordinator.class.getSimpleName());
    createEventThread();

    VerifiableProperties coordinatorProperties = new VerifiableProperties(_config.getConfigProperties());
    _eventProducerConfig = coordinatorProperties.getDomainProperties(EVENT_PRODUCER_CONFIG_DOMAIN);

    _cpProvider = new ZookeeperCheckpointProvider(_adapter);
    _metrics = new CoordinatorMetrics(this);

    // Callback initialization – helps fetch throughput violating topics for given datastreams dynamically in runtime.
    _throughputViolatingTopicsProvider =
        isThroughputViolatingTopicsHandlingEnabled() ? (t) -> getThroughputViolatingTopics(t.getDatastreams())
            : (t) -> new HashSet<>();
  }

  @VisibleForTesting
  ZkAdapter createZkAdapter() {
    return new ZkAdapter(_config.getZkAddress(), _clusterName, _config.getDefaultTransportProviderName(),
        _config.getZkSessionTimeout(), _config.getZkConnectionTimeout(), _config.getDebounceTimerMs(), this);
  }

  /**
   * Start Coordinator (and all connectors)
   */
  public void start() {
    _log.info("Starting coordinator");
    startEventThread();
    _adapter.connect(_config.getReinitOnNewZkSession());

    // Initializing executor services
    _scheduledExecutor = Executors.newSingleThreadScheduledExecutor(
        new ThreadFactoryBuilder().setNameFormat("CoordinatorScheduledExecutor-%d").build());
    _tokenClaimExecutor = Executors.newFixedThreadPool(TOKEN_CLAIM_THREAD_POOL_SIZE,
        new ThreadFactoryBuilder().setNameFormat("CoordinatorTokenClaimExecutor-%d").build());

    for (String connectorType : _connectors.keySet()) {
      ConnectorInfo connectorInfo = _connectors.get(connectorType);
      ConnectorWrapper connector = connectorInfo.getConnector();

      // Creating a separate thread pool for making the onAssignmentChange calls to the connector
      _assignmentChangeThreadPool.put(connectorType, Executors.newSingleThreadExecutor());

      // populate the instanceName. We only know the instance name after _adapter.connect()
      connector.setInstanceName(getInstanceName());

      // make sure connector znode exists upon instance start. This way in a brand new cluster
      // we can inspect ZooKeeper and know what connectors are created
      _adapter.addConnectorType(connector.getConnectorType());

      // call connector::start API
      connector.start(connectorInfo.getCheckpointProvider());

      _log.info("Coordinator started");
    }

    // now that instance is started, make sure it doesn't miss any assignment created during
    // the slow startup
    queueHandleAssignmentOrDatastreamChangeEvent(CoordinatorEvent.createHandleAssignmentChangeEvent(), true);

    // Queue up one heartbeat per period with a initial delay of 3 periods
    _scheduledExecutor.scheduleAtFixedRate(() -> _eventQueue.put(CoordinatorEvent.HEARTBEAT_EVENT),
        _heartbeatPeriod.toMillis() * 3, _heartbeatPeriod.toMillis(), TimeUnit.MILLISECONDS);
  }

  private synchronized void createEventThread() {
    _eventThread = new CoordinatorEventProcessor();
    _eventThread.setDaemon(true);
  }

  private synchronized void startEventThread() {
    if (!_shutdown) {
      _eventThread.start();
    }
  }

  private synchronized boolean stopEventThread() {
    // interrupt the thread if it's not gracefully shutdown
    while (_eventThread.isAlive()) {
      try {
        _eventThread.interrupt();
        _eventThread.join(EVENT_THREAD_SHORT_JOIN_TIMEOUT);
      } catch (InterruptedException e) {
        _log.warn("Exception caught while interrupting the event thread", e);
        return true;
      }
    }
    return false;
  }

  private synchronized boolean waitForEventThreadToJoin() {
    try {
      _eventThread.join(EVENT_THREAD_LONG_JOIN_TIMEOUT);
    } catch (InterruptedException e) {
      _log.warn("Exception caught while waiting the event thread to stop", e);
      return true;
    }
    return false;
  }

  /**
   * Stop coordinator (and all connectors)
   */
  public void stop() {
    _log.info("Stopping coordinator");

    _shutdown = true;

    // queue a NO_OP event to unblock eventThread if it is waiting on the queue
    _eventQueue.put(CoordinatorEvent.NO_OP_EVENT);

    // wait for eventThread to gracefully finish
    if (waitForEventThreadToJoin()) {
      return;
    }

    if (stopEventThread()) {
      return;
    }

    // Stopping all the connectors so that they stop producing.
    for (String connectorType : _connectors.keySet()) {
      try {
        ConnectorInfo connectorInfo = _connectors.get(connectorType);
        connectorInfo.getConnector().stop();
        connectorInfo.getAssignmentStrategy().cleanupStrategy();
      } catch (Exception ex) {
        _log.warn(String.format(
            "Connector stop threw an exception for connectorType %s, " + "Swallowing it and continuing shutdown.",
            connectorType), ex);
      }
    }

    // Shutdown executor services
    if (_scheduledExecutor != null) {
      _scheduledExecutor.shutdown();
    }
    if (_tokenClaimExecutor != null) {
      _tokenClaimExecutor.shutdown();
    }

    // Shutdown the event producer.
    for (DatastreamTask task : _assignedDatastreamTasks.values()) {
      ((EventProducer) task.getEventProducer()).shutdown(false);
    }

    //Stop all the Transport provider admins.
    for (TransportProviderAdmin tpAdmin : _transportProviderAdmins.values()) {
      tpAdmin.stop();
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
    // verify/clean up the orphan task nodes under connector should be called only once after becoming leader,
    // since it is an expensive operation. So, passing cleanUpOrphanNodes = true only on onBecomeLeader.
    _eventQueue.put(CoordinatorEvent.createLeaderDoAssignmentEvent(true));
    _log.info("Coordinator::onBecomeLeader completed successfully");
  }

  /**
   * {@inheritDoc}
   * This method is called when a new datastream server is added or existing datastream server goes down.
   */
  @Override
  public void onLiveInstancesChange() {
    _log.info("Coordinator::onLiveInstancesChange is called");
    _eventQueue.put(CoordinatorEvent.createLeaderDoAssignmentEvent(false));
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
    _eventQueue.put(CoordinatorEvent.createLeaderDoAssignmentEvent(false));
    _log.info("Coordinator::onDatastreamAddOrDrop completed successfully");
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void onDatastreamUpdate() {
    _log.info("Coordinator::onDatastreamUpdate is called");
    List<DatastreamGroup> datastreamGroups;
    // We need this synchronization to protect the updates on _assignedDatastreamTasks
    synchronized (_assignedDatastreamTasks) {
      // On datastream update the CachedDatastreamReader won't refresh its data, so we need to invalidate the cache
      _datastreamCache.invalidateAllCache();
      datastreamGroups = _datastreamCache.getDatastreamGroups();
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

    if (isThroughputViolatingTopicsHandlingEnabled()) {
      _log.info(
          "Populating the datastream violating topics to host level cache from the datastream objects on the update trigger");
      try {
        populateThroughputViolatingTopicsMap(datastreamGroups);
      } catch (Exception exception) {
        _log.error(
            "Received an exception while populating the datastream violating topics to host level cache from the "
                + "datastream objects on the update trigger", exception);
      }
    }
    queueHandleAssignmentOrDatastreamChangeEvent(CoordinatorEvent.createHandleDatastreamChangeEvent(), true);
    _log.info("Coordinator::onDatastreamUpdate completed successfully");
  }

  // This helper function populates the violations to local cache from the datastream metadata on every update call.
  // Also note that updates to this local cache follows the behavior of replace-all, and not incremental.
  private void populateThroughputViolatingTopicsMap(List<DatastreamGroup> datastreamGroups) {
    _throughputViolatingTopicsMapWriteLock.lock();
    try {
      // clearing the cache as we would only maintain the latest reported information everytime.
      _throughputViolatingTopicsMap.clear();

      // fetching new violations from the datastream object.
      datastreamGroups.forEach(datastreamGroup -> datastreamGroup.getDatastreams().forEach(datastream -> {
        if (!Objects.requireNonNull(datastream.getMetadata()).containsKey(THROUGHPUT_VIOLATING_TOPICS)) {
          // if the throughput violating metadata field does not exist, we skip handling logic and reporting metrics
          return;
        }
        // parse csv formatted violations metadata-string for every datastream
        String commaSeparatedViolatingTopics = datastream.getMetadata().get(THROUGHPUT_VIOLATING_TOPICS);
        String[] violatingTopics = Arrays.stream(commaSeparatedViolatingTopics.split(","))
            .filter(s -> !s.trim().isEmpty())
            .toArray(String[]::new);

        if (violatingTopics.length > 0) {
          _throughputViolatingTopicsMap.put(datastream.getName(), new HashSet<>(Arrays.asList(violatingTopics)));
          _log.info("For datastream {}, Successfully reported throughput violating topics : {}", datastream.getName(),
              violatingTopics);
        }
        _metrics.registerOrSetKeyedGauge(datastream.getName(),
            CoordinatorMetrics.NUM_THROUGHPUT_VIOLATING_TOPICS_PER_DATASTREAM, () -> violatingTopics.length);
      }));
    } finally {
      _throughputViolatingTopicsMapWriteLock.unlock();
    }
  }

  /**
   * Fetch all the throughput violating topics per datastream from cache.
   * */
  @VisibleForTesting
  Set<String> getThroughputViolatingTopics(List<Datastream> datastreams) {
    _throughputViolatingTopicsMapReadLock.lock();
    try {
      return datastreams.stream()
          .flatMap(
              datastream -> _throughputViolatingTopicsMap.getOrDefault(datastream.getName(), new HashSet<>()).stream())
          .collect(Collectors.toSet());
    } finally {
      _throughputViolatingTopicsMapReadLock.unlock();
    }
  }

  // This feature enables handling the management of throughput violating topics.
  // Latency metrics and SLAs would be reported separately for these topics if their
  // per partition throughput is not within brooklin's permissible bounds.
  public boolean isThroughputViolatingTopicsHandlingEnabled() {
    return _config.getEnableThroughputViolatingTopicsHandling();
  }

  /**
   * onPartitionMovement is called when partition movement info has been put into zookeeper
   */
  @Override
  public void onPartitionMovement(Long notifyTimestamp) {
    _log.info("Coordinator::onPartitionMovement is called");
    _eventQueue.put(CoordinatorEvent.createPartitionMovementEvent(notifyTimestamp));
    _log.info("Coordinator::onPartitionMovement completed successfully");
  }

  /**
   * {@inheritDoc}
   * Stop all the tasks and wait for new session to connect.
   */
  @Override
  public void onSessionExpired() {
    _log.info("Coordinator::onSessionExpired is called");

    if (_zkSessionExpired) {
      _log.info("session expiry is already handled. return");
      return;
    }
    _zkSessionExpired = true;

    if (_shutdown) {
      return;
    }
    stopEventThread();

    _leaderDatastreamAddOrDeleteEventScheduled.set(false);
    if (_leaderDatastreamAddOrDeleteEventScheduledFuture != null) {
      _leaderDatastreamAddOrDeleteEventScheduledFuture.cancel(true);
      _leaderDatastreamAddOrDeleteEventScheduledFuture = null;
    }

    _leaderDoAssignmentScheduled.set(false);
    if (_leaderDoAssignmentScheduledFuture != null) {
      _leaderDoAssignmentScheduledFuture.cancel(true);
      _leaderDoAssignmentScheduledFuture = null;
    }

    _eventQueue.clear();

    // Stopping all the connectors so that they stop producing.
    List<Future<Boolean>> assignmentChangeFutures = _connectors.keySet().stream()
        .map(connectorType -> {
          _assignmentChangeThreadPool.get(connectorType).shutdownNow();
          _assignmentChangeThreadPool.put(connectorType, Executors.newSingleThreadExecutor());
          return dispatchAssignmentChangeIfNeeded(connectorType, new ArrayList<>(), false, false);
        })
        .filter(Objects::nonNull)
        .collect(Collectors.toList());

    onDatastreamChange(new ArrayList<>());

    // Perform AssignmentStrategy cleanup for all the connectors
    _connectors.values().forEach(connectorInfo -> {
      connectorInfo.getAssignmentStrategy().cleanupStrategy();
    });

    // Shutdown the event producer to stop any further production of records.
    // Event producer shutdown sequence does not need to wait for onAssignmentChange to complete.
    // This will ensure that even if any task thread does not respond to thread interruption, it will
    // still not be able to produce any records to destination.
    for (DatastreamTask task : _assignedDatastreamTasks.values()) {
      // skipping the checkpoint update as zookeeper session has expired and the current thread will
      // get stuck waiting for zookeeper session to be connected. So, skipping the checkpoint update.
      ((EventProducer) task.getEventProducer()).shutdown(true);
    }

    // Wait till all the futures are complete or timeout.
    ExecutorService threadPoolExecutor = Executors.newFixedThreadPool(1);
    threadPoolExecutor.submit(() -> {
      Instant start = Instant.now();
      try {
        getAssignmentsFuture(assignmentChangeFutures, start);
      } catch (Exception e) {
        _log.warn("Hit exception while clearing the assignment list", e);
      } finally {
        assignmentChangeFutures.forEach(future -> future.cancel(true));
      }
    });

    _assignedDatastreamTasks.clear();
    _log.info("Coordinator::onSessionExpired completed successfully.");
  }

  @VisibleForTesting
  boolean isZkSessionExpired() {
    return _zkSessionExpired;
  }

  @Override
  public void onNewSession() {
    createEventThread();
    startEventThread();
    _adapter.connect(true);
    // ensure it doesn't miss any assignment created
    _eventQueue.put(CoordinatorEvent.createHandleAssignmentChangeEvent());

    // Queue up one heartbeat per period with a initial delay of 3 periods
    _scheduledExecutor.scheduleAtFixedRate(() -> _eventQueue.put(CoordinatorEvent.HEARTBEAT_EVENT),
        _heartbeatPeriod.toMillis() * 3, _heartbeatPeriod.toMillis(), TimeUnit.MILLISECONDS);

    _zkSessionExpired = false;
  }

  private void getAssignmentsFuture(List<Future<Boolean>> assignmentChangeFutures, Instant start)
      throws TimeoutException, InterruptedException {
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
    queueHandleAssignmentOrDatastreamChangeEvent(CoordinatorEvent.createHandleAssignmentChangeEvent(), true);

    if (isThroughputViolatingTopicsHandlingEnabled()) {
      try {
        // On creating a datastream if the metadata contains any throughput violating topics, we populate the host level cache
        List<DatastreamGroup> datastreamGroups = _adapter.getInstanceAssignment(_adapter.getInstanceName())
            .stream()
            .map(task -> new DatastreamGroup(getDatastreamTask(task).getDatastreams()))
            .collect(Collectors.toList());
        _log.info(
            "Populating the datastream violating topics to host level cache from the datastream objects on the create trigger");
        populateThroughputViolatingTopicsMap(datastreamGroups);
      } catch (Exception exception) {
        _log.error(
            "Received an exception while populating the datastream violating topics to host level cache from the "
                + "datastream objects on the create trigger", exception);
      }
    }
    _log.info("Coordinator::onAssignmentChange completed successfully");
  }

  private int getAssignmentTaskCount(Map<String, List<DatastreamTask>> assignment) {
    return assignment.values().stream().mapToInt(List::size).sum();
  }

  private void queueHandleAssignmentOrDatastreamChangeEvent(CoordinatorEvent event, boolean isReset) {
    _eventQueue.put(event);
    if (isReset) {
      _assignmentRetryCount.set(0);
    } else {
      _assignmentRetryCount.incrementAndGet();
    }
  }

  private void retryHandleAssignmentChange(boolean isDatastreamUpdate) {
    boolean queueEvent = true;
    if (_assignmentRetryCount.compareAndSet(_config.getMaxAssignmentRetryCount(), 0)) {
      _log.error(
          "onAssignmentChange retries reached threshold of {}. Not queuing further retry on onAssignmentChange event.",
          _config.getMaxAssignmentRetryCount());
      queueEvent = false;
    }

    EventType meter = isDatastreamUpdate ? HANDLE_DATASTREAM_CHANGE_WITH_UPDATE : HANDLE_ASSIGNMENT_CHANGE;
    _log.warn("Updating metric for event " + meter);
    _metrics.updateKeyedMeter(CoordinatorMetrics.getKeyedMeter(meter), 1);

    if (queueEvent) {
      _log.warn("Queuing onAssignmentChange event");
      CoordinatorEvent event = isDatastreamUpdate ? CoordinatorEvent.createHandleDatastreamChangeEvent() :
          CoordinatorEvent.createHandleAssignmentChangeEvent();
      queueHandleAssignmentOrDatastreamChangeEvent(event, false);
    }
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

    int totalTasks = getAssignmentTaskCount(currentAssignment);

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
        .map(connectorType -> dispatchAssignmentChangeIfNeeded(connectorType, new ArrayList<>(), isDatastreamUpdate, true))
        .filter(Objects::nonNull)
        .collect(Collectors.toList());

    // case (2) - Dispatch all the assignment changes in a separate thread
    assignmentChangeFutures.addAll(newConnectorList.stream()
        .map(connectorType -> dispatchAssignmentChangeIfNeeded(connectorType, currentAssignment.get(connectorType),
            isDatastreamUpdate, true))
        .filter(Objects::nonNull)
        .collect(Collectors.toList()));

    int submittedTasks = getAssignmentTaskCount(currentAssignment);

    // Wait till all the futures are complete or timeout.
    Instant start = Instant.now();
    try {
      getAssignmentsFuture(assignmentChangeFutures, start);
    } catch (TimeoutException e) {
      // if it's timeout then we will retry
      _log.warn("Timeout when doing the assignment", e);
      retryHandleAssignmentChange(isDatastreamUpdate);
      return;
    } catch (InterruptedException e) {
      _log.warn("onAssignmentChange call got interrupted", e);
    } finally {
      assignmentChangeFutures.forEach(future -> future.cancel(true));
    }

    // now save the current assignment
    List<DatastreamTask> oldAssignment = new ArrayList<>(_assignedDatastreamTasks.values());
    _assignedDatastreamTasks.clear();
    _assignedDatastreamTasks.putAll(currentAssignment.values()
        .stream()
        .flatMap(Collection::stream)
        .collect(Collectors.toMap(DatastreamTask::getDatastreamTaskName, Function.identity(),
            (existingTask, duplicateTask) -> existingTask)));
    List<DatastreamTask> newAssignment = new ArrayList<>(_assignedDatastreamTasks.values());

    if ((totalTasks - submittedTasks) > 0) {
      _log.warn("Failed to submit {} tasks from currentAssignment. Queueing onAssignmentChange event again",
          totalTasks - submittedTasks);
      // Update the metric and queue the event only once for all the tasks that failed to be initialized
      retryHandleAssignmentChange(isDatastreamUpdate);
    }

    if (_config.getEnableAssignmentTokens()) {
      try {
        // Queue assignment token claim task
        List<String> oldAssignmentTaskNames = oldAssignment.stream().map(DatastreamTask::getDatastreamTaskName).
            collect(Collectors.toList());
        List<String> newAssignmentTaskNames = newAssignment.stream().map(DatastreamTask::getDatastreamTaskName).
            collect(Collectors.toList());
        _log.debug("Claiming assignment tokens. Old assignment: {}", oldAssignmentTaskNames);
        _log.debug("Claiming assignment tokens. New assignment: {}", newAssignmentTaskNames);
        _tokenClaimExecutor.submit(() ->
            maybeClaimAssignmentTokensForStoppingStreams(newAssignment, oldAssignment));
      } catch (RejectedExecutionException ex) {
        _log.warn("Failed to submit the task for claiming assignment tokens", ex);
      }
    }

    _log.info("END: Coordinator::handleAssignmentChange, Duration: {} milliseconds",
        System.currentTimeMillis() - startAt);
    _metrics.updateMeter(CoordinatorMetrics.Meter.NUM_ASSIGNMENT_CHANGES, 1);
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
      boolean isDatastreamUpdate, boolean retryAndSaveError) {
    // Failed datastream tasks
    Set<DatastreamTask> failedDatastreamTasks = new HashSet<>();
    ConnectorInfo connectorInfo = _connectors.get(connectorType);
    ConnectorWrapper connector = connectorInfo.getConnector();

    List<DatastreamTask> oldAssignment = _assignedDatastreamTasks.values()
        .stream()
        .filter(t -> t.getConnectorType().equals(connectorType))
        .collect(Collectors.toList());
    List<DatastreamTask> addedTasks = getAddedTasks(assignment, oldAssignment);
    List<DatastreamTask> removedTasks = getRemovedTasks(assignment, oldAssignment);

    // if there are any difference in the list of assignment. Note that if there are no difference
    // between the two lists, then the connector onAssignmentChange() is not called.
    if (isDatastreamUpdate || !addedTasks.isEmpty() || !removedTasks.isEmpty()) {
      // Populate the event producers before calling the connector with the list of tasks.
      addedTasks.stream()
          .filter(t -> t.getEventProducer() == null)
          .forEach(task -> initializeTask(task, failedDatastreamTasks, retryAndSaveError));
      assignment.removeAll(failedDatastreamTasks);
      return submitAssignment(connectorType, assignment, isDatastreamUpdate, connector, removedTasks, retryAndSaveError);
    }

    return null;
  }

  private List<DatastreamTask> getRemovedTasks(List<DatastreamTask> newAssignment, List<DatastreamTask> oldAssignment) {
    Set<DatastreamTask> removedTasks = new HashSet<>(oldAssignment);
    newAssignment.forEach(removedTasks::remove);
    return new ArrayList<>(removedTasks);
  }

  private List<DatastreamTask> getAddedTasks(List<DatastreamTask> newAssignment, List<DatastreamTask> oldAssignment) {
    Set<DatastreamTask> addedTasks = new HashSet<>(newAssignment);
    oldAssignment.forEach(addedTasks::remove);
    return new ArrayList<>(addedTasks);
  }

  @VisibleForTesting
  void maybeClaimAssignmentTokensForStoppingStreams(List<DatastreamTask> newAssignment,
      List<DatastreamTask> oldAssignment) {
    Map<String, List<DatastreamTask>> newAssignmentPerConnector = new HashMap<>();
    for (DatastreamTask task : newAssignment) {
      String connectorType = task.getConnectorType();
      newAssignmentPerConnector.computeIfAbsent(connectorType, k -> new ArrayList<>()).add(task);
    }

    Map<String, List<DatastreamTask>> oldAssignmentPerConnector = new HashMap<>();
    for (DatastreamTask task : oldAssignment) {
      String connectorType = task.getConnectorType();
      oldAssignmentPerConnector.computeIfAbsent(connectorType, k -> new ArrayList<>()).add(task);
    }

    Set<String> allConnectors = new HashSet<>();
    allConnectors.addAll(newAssignmentPerConnector.keySet());
    allConnectors.addAll(oldAssignmentPerConnector.keySet());

    // The follower nodes don't keep an up-to-date view of the datastreams Zk node and making them listen to changes
    // to datastreams node risks overwhelming the ZK server with reads. Instead, the follower is inferring the stopping
    // streams from the task assignment for each connector type. Even if that inference results in a falsely identified
    // stopping stream, the attempt to claim the token will be a no-op.
    allConnectors.parallelStream().forEach(connector -> {
      List<DatastreamTask> oldTasks = oldAssignmentPerConnector.getOrDefault(connector, Collections.emptyList());
      List<DatastreamTask> newTasks = newAssignmentPerConnector.getOrDefault(connector, Collections.emptyList());
      List<DatastreamTask> removedTasks = getRemovedTasks(newTasks, oldTasks);
      List<String> removedTaskNames = removedTasks.stream().map(DatastreamTask::getDatastreamTaskName).
          collect(Collectors.toList());
      _log.debug("Removed tasks from connector {}: {}", connector, removedTaskNames);

      List<Datastream> stoppingStreams = Collections.emptyList();
      Set<String> stoppingStreamNames = Collections.emptySet();

      try {
        stoppingStreams = inferStoppingDatastreamsFromAssignment(newTasks, removedTasks);
        stoppingStreamNames = stoppingStreams.stream().map(Datastream::getName).collect(Collectors.toSet());
      } catch (Exception ex) {
        _log.error("Failed to infer stopping streams. ", ex);
      }

      if (!stoppingStreamNames.isEmpty()) {
        _metrics.updateMeter(CoordinatorMetrics.Meter.NUM_INFERRED_STOPPING_STREAMS, stoppingStreams.size());
        _log.info("Trying to claim assignment tokens for connector {}, streams: {}", connector, stoppingStreamNames);

        Set<String> finalStoppingStreamNames = stoppingStreamNames;
        Set<String> stoppingDatastreamTasks = removedTasks.stream().
            filter(t -> finalStoppingStreamNames.contains(t.getTaskPrefix())).
            map(DatastreamTask::getId).collect(Collectors.toSet());

        // TODO Evaluate whether we need to optimize here and make this call for each datastream
        try {
          if (PollUtils.poll(() -> connectorTasksHaveStopped(connector, stoppingDatastreamTasks),
              _config.getTaskStopCheckRetryPeriodMs(), _config.getTaskStopCheckTimeoutMs())) {
            _adapter.claimAssignmentTokensForDatastreams(stoppingStreams, _adapter.getInstanceName(), false);
          } else {
            _log.warn("Connector {} failed to stop its tasks in {}ms. No assignment tokens will be claimed",
                connector, _config.getTaskStopCheckTimeoutMs());
          }
        } catch (Exception ex) {
          _log.error("Failed to claim assignment tokens for stopping streams:", ex);
        }
      } else {
        _log.info("No streams have been inferred as stopping for connector {} and no assignment tokens will be claimed",
            connector);
      }
    });
  }

  @VisibleForTesting
  boolean connectorTasksHaveStopped(String connectorName, Set<String> stoppingTasks) {
    Set<String> activeTasks =
        new HashSet<>(_connectors.get(connectorName).getConnector().getConnectorInstance().getActiveTasks());
    return activeTasks.isEmpty() || stoppingTasks.isEmpty() || Collections.disjoint(activeTasks, stoppingTasks);
  }

  @VisibleForTesting
  static List<Datastream> inferStoppingDatastreamsFromAssignment(List<DatastreamTask> newAssignment,
      List<DatastreamTask> removedTasks) {
    Map<String, Set<Datastream>> taskPrefixToDatastream = new HashMap<>();
    for (DatastreamTask task : removedTasks) {
      taskPrefixToDatastream.computeIfAbsent(task.getTaskPrefix(), k -> new HashSet<>()).
          addAll(task.getDatastreams());
    }

    Set<String> removedPrefixes = removedTasks.stream().map(DatastreamTask::getTaskPrefix).collect(Collectors.toSet());
    Set<String> activePrefixes = newAssignment.stream().map(DatastreamTask::getTaskPrefix).collect(Collectors.toSet());
    removedPrefixes.removeAll(activePrefixes);

    List<Datastream> stoppingStreams = removedPrefixes.stream().map(taskPrefixToDatastream::get).
        flatMap(Set::stream).collect(Collectors.toList());
    return stoppingStreams;
  }

  private Future<Boolean> submitAssignment(String connectorType, List<DatastreamTask> assignment,
      boolean isDatastreamUpdate, ConnectorWrapper connector, List<DatastreamTask> removedTasks, boolean retryAndSaveError) {
    // Dispatch the onAssignmentChange to the connector in a separate thread.
    return _assignmentChangeThreadPool.get(connectorType).submit(() -> {
      try {
        // Send a new copy of assignment to connector to ensure that assignment is not modified.
        // Any modification to assignment object directly will cause discrepancy in the current assignment list
        connector.onAssignmentChange(new ArrayList<>(assignment));
        // Unassign tasks with producers
        uninitializeTasks(removedTasks);
      } catch (Exception ex) {
        String err = String.format("connector.onAssignmentChange for connector %s threw an exception", connectorType);
        if (retryAndSaveError) {
          err += " Queuing up a new onAssignmentChange event for retry.";
          _eventQueue.put(CoordinatorEvent.createHandleInstanceErrorEvent(ExceptionUtils.getRootCauseMessage(ex)));
          CoordinatorEvent event = isDatastreamUpdate ? CoordinatorEvent.createHandleDatastreamChangeEvent() :
              CoordinatorEvent.createHandleAssignmentChangeEvent();
          queueHandleAssignmentOrDatastreamChangeEvent(event, false);
        }
        _log.warn(err, ex);
        return false;
      }
      return true;
    });
  }

  private void uninitializeTasks(List<DatastreamTask> tasks) {

    Map<String, List<DatastreamTask>> datastreamTasksPerTransportProvider =
        tasks.stream().collect(Collectors.groupingBy(DatastreamTask::getTransportProviderName, Collectors.toList()));

    datastreamTasksPerTransportProvider.forEach((transportProviderName, datastreamTaskList) -> {
      TransportProviderAdmin tpAdmin = _transportProviderAdmins.get(transportProviderName);
      tpAdmin.unassignTransportProvider(datastreamTaskList);
    });

    tasks.forEach(_cpProvider::unassignDatastreamTask);
  }

  private void initializeTask(DatastreamTask task, Set<DatastreamTask> failedDatastreamTasks, boolean retryAndSaveError) {
    try {
      DatastreamTaskImpl taskImpl = (DatastreamTaskImpl) task;
      assignSerdes(taskImpl);

      boolean customCheckpointing = getCustomCheckpointing(task);
      TransportProviderAdmin tpAdmin = _transportProviderAdmins.get(task.getTransportProviderName());
      TransportProvider transportProvider = tpAdmin.assignTransportProvider(task);

      EventProducer producer =
          new EventProducer(task, transportProvider, _cpProvider, _eventProducerConfig, customCheckpointing,
              _throughputViolatingTopicsProvider);

      taskImpl.setEventProducer(producer);
      Map<Integer, String> checkpoints = producer.loadCheckpoints(task);
      taskImpl.setCheckpoints(checkpoints);
    } catch (Exception e) {
      _log.error("Failed to initialize task: " + task.getDatastreamTaskName(), e);
      if (retryAndSaveError) {
        _eventQueue.put(CoordinatorEvent.createHandleInstanceErrorEvent(ExceptionUtils.getRootCauseMessage(e)));
        failedDatastreamTasks.add(task);
      }
    }
  }

  private boolean getCustomCheckpointing(DatastreamTask task) {
    boolean customCheckpointing = _connectors.get(task.getConnectorType()).isCustomCheckpointing();

    Datastream datastream = task.getDatastreams().get(0);
    if (datastream.hasMetadata() &&
        Objects.requireNonNull(datastream.getMetadata()).containsKey(DatastreamMetadataConstants.CUSTOM_CHECKPOINT)) {
      customCheckpointing = Boolean.parseBoolean(
          datastream.getMetadata().get(DatastreamMetadataConstants.CUSTOM_CHECKPOINT));
      _log.info(String.format("Custom checkpointing overridden by metadata to be: %b for datastream: %s",
          customCheckpointing, datastream));
    }

    return customCheckpointing;
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
    boolean isLeader = _adapter.isLeader();
    if (!isLeader && isLeaderEvent(event.getType())) {
      _log.info("Skipping event {} isLeader: false", event.getType());
      _log.info("END: Handle event " + event);
      return;
    }
    try {
      switch (event.getType()) {
        case LEADER_DO_ASSIGNMENT:
          handleLeaderDoAssignment((Boolean) event.getEventMetadata());
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
          if (event.getEventMetadata() == null) {
            _log.error("Datastream group is not found when performing partition assignment, ignore the assignment");
          } else {
            performPartitionAssignment((String) event.getEventMetadata());
          }
          break;

        case LEADER_PARTITION_MOVEMENT:
          performPartitionMovement((Long) event.getEventMetadata());
          break;

        default:
          String errorMessage = String.format("Unknown event type %s.", event.getType());
          ErrorLogger.logAndThrowDatastreamRuntimeException(_log, errorMessage, null);
          break;
      }
    } catch (Exception e) {
      _metrics.updateKeyedMeter(CoordinatorMetrics.getKeyedMeter(event.getType()), 1);
      _log.error("ERROR: event + " + event + " failed.", e);
    }

    _log.info("END: Handle event " + event);
  }

  private boolean isLeaderEvent(CoordinatorEvent.EventType eventType) {
    switch (eventType) {
      case LEADER_DO_ASSIGNMENT:
      case HANDLE_ADD_OR_DELETE_DATASTREAM:
      case LEADER_PARTITION_ASSIGNMENT:
      case LEADER_PARTITION_MOVEMENT:
        return true;
      default:
        return false;
    }
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
    _metrics.updateCounter(CoordinatorMetrics.Counter.NUM_HEARTBEATS, 1);
  }

  /**
   * Check if a datastream is either marked as deleting or its TTL has expired
   */
  private boolean isDeletingOrExpired(Datastream stream) {
    boolean isExpired = false;

    // Check TTL
    if (Objects.requireNonNull(stream.getMetadata()).containsKey(TTL_MS) && stream.getMetadata().containsKey(CREATION_MS)) {
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
    _log.info("START: Coordinator::handleDatastreamAddOrDelete.");

    // Get the list of all datastreams
    List<Datastream> allStreams = _datastreamCache.getAllDatastreams(true);
    // List of active streams that are not expired or deleted. Used for checking for a duplicate stream when deciding
    // whether to delete a datastream tasks and topic or not.
    List<Datastream> activeStreams = allStreams.stream().filter(ds -> !isDeletingOrExpired(ds)).collect(Collectors.toList());

    // do nothing if there are zero datastreams
    if (allStreams.isEmpty()) {
      _log.warn("Received a new datastream event, but there were no datastreams");
      _log.info("END: Coordinator::handleDatastreamAddOrDelete.");
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

        hardDeleteDatastream(ds, activeStreams);
      }
    }

    if (shouldRetry) {
      _metrics.updateKeyedMeter(CoordinatorMetrics.KeyedMeter.HANDLE_DATASTREAM_ADD_OR_DELETE_NUM_RETRIES, 1);

      // If there are any failure, we will need to schedule retry if
      // there is no pending retry scheduled already.
      if (_leaderDatastreamAddOrDeleteEventScheduled.compareAndSet(false, true)) {
        _log.warn("Schedule retry for handling new datastream");
        _leaderDatastreamAddOrDeleteEventScheduledFuture = _scheduledExecutor.schedule(() -> {
          _eventQueue.put(CoordinatorEvent.createHandleDatastreamAddOrDeleteEvent());

          // Allow further retry scheduling
          _leaderDatastreamAddOrDeleteEventScheduled.set(false);
        }, _config.getRetryIntervalMs(), TimeUnit.MILLISECONDS);
      }
    }

    _eventQueue.put(CoordinatorEvent.createLeaderDoAssignmentEvent(false));
    _log.info("END: Coordinator::handleDatastreamAddOrDelete.");
  }

  private void hardDeleteDatastream(Datastream ds, List<Datastream> activeStreams) {
    String taskPrefix;
    if (DatastreamUtils.containsTaskPrefix(ds)) {
      taskPrefix = DatastreamUtils.getTaskPrefix(ds);
    } else {
      taskPrefix = DatastreamTaskImpl.getTaskPrefix(ds);
    }

    Optional<Datastream> duplicateStream = activeStreams.stream()
        .filter(DatastreamUtils::containsTaskPrefix)
        .filter(x -> !x.getName().equals(ds.getName()) && DatastreamUtils.getTaskPrefix(x).equals(taskPrefix))
        .findFirst();

    if (!duplicateStream.isPresent()) {
      _log.info(
          "No datastream left in the datastream group with taskPrefix {}. Deleting all tasks corresponding to the datastream.",
          taskPrefix);
      _adapter.deleteTasksWithPrefix(ds.getConnectorName(), taskPrefix);
      deleteTopic(ds);
    } else {
      _log.info("Found duplicate datastream {} for the datastream to be deleted {}. Not deleting the tasks.",
          duplicateStream.get().getName(), ds.getName());
    }

    _adapter.deleteDatastream(ds.getName());
  }

  /**
   * Invokes post datastream state change action of connector for given datastream.
   * @param datastream the datastream
   * @throws DatastreamException if fails to perform post datastream action
   */
  public void invokePostDataStreamStateChangeAction(final Datastream datastream) throws DatastreamException {
    _log.info("Invoke post datastream state change action");
    try {
      Datastream datastreamCopy = datastream.copy();
      final String connectorName = datastreamCopy.getConnectorName();
      final ConnectorInfo connectorInfo = _connectors.get(connectorName);
      connectorInfo.getConnector().postDatastreamStateChangeAction(datastreamCopy);
    } catch (CloneNotSupportedException e) {
      _log.error("Failed to copy object for datastream={}", datastream.getName());
      throw new DatastreamException("Failed to copy datastream object", e);
    } catch (DatastreamException e) {
      _log.error("Failed to perform post datastream state change action datastream={}", datastream.getName());
      _metrics.updateKeyedMeter(CoordinatorMetrics.KeyedMeter.POST_DATASTREAMS_STATE_CHANGE_ACTION_NUM_ERRORS, 1);
      throw e;
    }
  }

  private void createTopic(Datastream datastream) {
    _transportProviderAdmins.get(datastream.getTransportProviderName()).createDestination(datastream);

    // For deduped datastreams, all destination-related metadata have been copied by
    // populateDatastreamDestinationFromExistingDatastream().
    if (!Objects.requireNonNull(datastream.getMetadata()).containsKey(DatastreamMetadataConstants.DESTINATION_CREATION_MS)) {
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
    // Get all streams that are assignable, where status is READY or PAUSED. STOPPED or other datastream status will NOT get assigned
    return fetchDatastreamGroupsWithStatus(Arrays.asList(DatastreamStatus.READY, DatastreamStatus.PAUSED));
  }

  private List<DatastreamGroup> fetchDatastreamGroupsWithStatus(List<DatastreamStatus> requiredStatus) {
    // Get all streams that are assignable. Assignable datastreams are the ones:
    //  1) has a valid destination
    //  2) TTL has not expired
    // Note: We do not need to flush the cache, because the datastreams should have been read as part of the
    //       handleDatastreamAddOrDelete event (that should occur before handleLeaderDoAssignment)
    List<Datastream> allStreams = _datastreamCache.getAllDatastreams(false)
        .stream()
        .filter(datastream -> datastream.hasStatus() && requiredStatus.contains(datastream.getStatus())
            && hasValidDestination(datastream) && !isDeletingOrExpired(datastream))
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

  /*
   * If isNewlyElectedLeader is set to true, it cleans up the orphan connector tasks not assigned to
   * any instance after old unused tasks are cleaned up.
   */
  private void handleLeaderDoAssignment(boolean isNewlyElectedLeader) {
    boolean succeeded = true;
    _log.info("START: Coordinator::handleLeaderDoAssignment.");
    List<String> liveInstances = Collections.emptyList();
    Map<String, Set<DatastreamTask>> previousAssignmentByInstance = Collections.emptyMap();
    Map<String, List<DatastreamTask>> newAssignmentsByInstance = Collections.emptyMap();

    try {
      List<DatastreamGroup> datastreamGroups = fetchDatastreamGroups();
      List<DatastreamGroup> stoppingDatastreamGroups =
          fetchDatastreamGroupsWithStatus(Collections.singletonList(DatastreamStatus.STOPPING));
      _log.info("stopping datastreams: {}", stoppingDatastreamGroups);
      onDatastreamChange(datastreamGroups);

      if (isNewlyElectedLeader) {
        performPreAssignmentCleanup(datastreamGroups);
      }

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
      if (_config.getEnableAssignmentTokens() && !stoppingDatastreamGroups.isEmpty()) {
        _adapter.updateAllAssignmentsAndIssueTokens(newAssignmentsByInstance, stoppingDatastreamGroups);
        try {
          _tokenClaimExecutor.submit(() -> waitForStopToPropagateAndMarkDatastreamsStopped(stoppingDatastreamGroups,
              isNewlyElectedLeader));
        } catch (RejectedExecutionException ex) {
          _log.error("handleLeaderDoAssignment: Failed to submit the task that waits for stop propagation");
          succeeded = false;
        }

      } else {
        _adapter.updateAllAssignments(newAssignmentsByInstance);
        succeeded = markDatastreamsStopped(stoppingDatastreamGroups, Collections.emptySet());
      }
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
      _adapter.cleanUpDeadInstanceDataAndOtherUnusedTasks(previousAssignmentByInstance,
          newAssignmentsByInstance, instances);
      if (isNewlyElectedLeader) {
        performCleanupOrphanNodes();
      }
      _metrics.updateMeter(CoordinatorMetrics.Meter.NUM_REBALANCES, 1);
    }

    // schedule retry if failure
    if (!succeeded && !_leaderDoAssignmentScheduled.get()) {
      scheduleLeaderDoAssignmentRetry(isNewlyElectedLeader);
    }
    _log.info("END: Coordinator::handleLeaderDoAssignment.");
  }

  private void scheduleLeaderDoAssignmentRetry(boolean isNewlyElectedLeader) {
    _log.info("Schedule retry for leader assigning tasks");
    _metrics.updateKeyedMeter(CoordinatorMetrics.KeyedMeter.HANDLE_LEADER_DO_ASSIGNMENT_NUM_RETRIES, 1);
    _leaderDoAssignmentScheduled.set(true);
    // scheduling LEADER_DO_ASSIGNMENT event instantly to prevent any other event being handled before the reattempt.
    _leaderDoAssignmentScheduledFuture = _scheduledExecutor.schedule(() -> {
      _eventQueue.put(CoordinatorEvent.createLeaderDoAssignmentEvent(isNewlyElectedLeader), false);
      _leaderDoAssignmentScheduled.set(false);
    }, 0, TimeUnit.MILLISECONDS);
  }

  @VisibleForTesting
  void waitForStopToPropagateAndMarkDatastreamsStopped(List<DatastreamGroup> stoppingDatastreamGroups,
      boolean isNewlyElectedLeader) {
    List<String> streamNames = stoppingDatastreamGroups.stream().map(DatastreamGroup::getName).collect(
        Collectors.toList());
    _log.info("waitForStopToPropagateAndMarkDatastreamsStopped started in thread {} for streams {}",
        Thread.currentThread().getName(), streamNames);
    // Poll the zookeeper to ensure that hosts claimed assignment tokens for stopping streams
    Set<String> failedStreams = Collections.emptySet();
    if (_config.getEnableAssignmentTokens() &&
        !PollUtils.poll(() -> _adapter.getNumUnclaimedTokensForDatastreams(stoppingDatastreamGroups) == 0,
            STOP_PROPAGATION_RETRY_MS, _config.getStopPropagationTimeoutMs())) {
      Map<String, List<AssignmentToken>> unclaimedTokens =
          _adapter.getUnclaimedAssignmentTokensForDatastreams(stoppingDatastreamGroups);
      failedStreams = unclaimedTokens.keySet();
      Set<String> hosts = unclaimedTokens.values().stream().flatMap(List::stream).
          map(AssignmentToken::getIssuedFor).collect(Collectors.toSet());

      // We skip emitting the NUM_FAILED_STOPS metric in case of leader failover. This is because new leader may
      // issue extra tokens and revoke them later.
      if (!isNewlyElectedLeader && !failedStreams.isEmpty()) {
        _log.error("Stop failed to propagate within {}ms for streams: {}. The following hosts failed to claim their token(s): {}",
            _config.getStopPropagationTimeoutMs(), failedStreams, hosts);
        _metrics.updateMeter(CoordinatorMetrics.Meter.NUM_FAILED_STOPS, failedStreams.size());
      } else if (!failedStreams.isEmpty()) {
        _log.warn("Stop may have failed to propagate within {}ms for streams: {}. The newly elected leader was " +
                "expecting the hosts {} to claim tokens but they didn't",
            _config.getStopPropagationTimeoutMs(), failedStreams, hosts);
      }
      revokeUnclaimedAssignmentTokens(unclaimedTokens, stoppingDatastreamGroups);
    }

    // TODO Explore if the STOPPING -> STOPPED transition can be converted into an event type and scheduled in the event queue
    Set<String> finalFailedStreams = failedStreams;
    if (!PollUtils.poll(() -> markDatastreamsStopped(stoppingDatastreamGroups, finalFailedStreams),
        _config.getMarkDatastreamsStoppedRetryPeriodMs(), _config.getMarkDatastreamsStoppedTimeoutMs())) {
      _log.error("Failed to mark streams STOPPED within {}ms. Giving up.", _config.getMarkDatastreamsStoppedTimeoutMs());
    }
    _log.info("waitForStopToPropagateAndMarkDatastreamsStopped finished in thread {}", Thread.currentThread().getName());
  }

  private boolean markDatastreamsStopped(List<DatastreamGroup> stoppingDatastreamGroups, Set<String> failedStreams) {
    boolean success = true;
    boolean forceStop = _config.getForceStopStreamsOnFailure();
    Set<String> stoppingStreams =
        fetchDatastreamGroupsWithStatus(Collections.singletonList(DatastreamStatus.STOPPING)).
            stream().flatMap(dg -> dg.getDatastreams().stream()).map(Datastream::getName).
            collect(Collectors.toSet());
    for (DatastreamGroup datastreamGroup : stoppingDatastreamGroups) {
      for (Datastream datastream : datastreamGroup.getDatastreams()) {
        // Only streams that were confirmed to have stopped successfully will be transitioned to STOPPED state
        if (stoppingStreams.contains(datastream.getName()) &&
            (forceStop || !failedStreams.contains(datastream.getName()))) {
          datastream.setStatus(DatastreamStatus.STOPPED);
          _log.info("Transitioned datastream {} to STOPPED state", datastream.getName());
          if (!_adapter.updateDatastream(datastream)) {
            _log.error("Failed to update datastream: {} to stopped state", datastream.getName());
            success = false;
          }
        }
      }
    }
    return success;
  }

  private void revokeUnclaimedAssignmentTokens(Map<String, List<AssignmentToken>> unclaimedTokens,
      List<DatastreamGroup> stoppingDatastreamGroups) {
    _log.info("Revoking unclaimed tokens");
    Map<String, Datastream> datastreamMap = new HashMap<>();
    stoppingDatastreamGroups.forEach(dg -> dg.getDatastreams().forEach(ds -> datastreamMap.put(ds.getName(), ds)));
    for (String streamName : unclaimedTokens.keySet()) {
      Datastream stream = datastreamMap.get(streamName);

      if (stream == null) {
        _log.warn("Failed to claim token for unknown datastream: {}", streamName);
        continue;
      }

      List<String> instances = unclaimedTokens.get(streamName).stream().map(AssignmentToken::getIssuedFor).
          collect(Collectors.toList());
      instances.forEach(i -> _adapter.claimAssignmentTokensForDatastreams(Collections.singletonList(stream), i, true));
    }
  }

  protected void performPreAssignmentCleanup(List<DatastreamGroup> datastreamGroups) {

    // Map between instance to tasks assigned to the instance.
    Map<String, Set<DatastreamTask>> previousAssignmentByInstance = _adapter.getAllAssignedDatastreamTasks();

    _log.info("performPreAssignmentCleanup: start");
    _log.debug("performPreAssignmentCleanup: assignment before cleanup: " + previousAssignmentByInstance);

    for (String connectorType : _connectors.keySet()) {
      AssignmentStrategy strategy = _connectors.get(connectorType).getAssignmentStrategy();
      List<DatastreamGroup> datastreamsPerConnectorType = datastreamGroups.stream()
          .filter(x -> x.getConnectorName().equals(connectorType))
          .collect(Collectors.toList());

      Map<String, List<DatastreamTask>> tasksToCleanupMap = strategy.getTasksToCleanUp(datastreamsPerConnectorType,
          previousAssignmentByInstance);

      if (tasksToCleanupMap.size() > 0) {
        for (String instance : tasksToCleanupMap.keySet()) {
          List<String> tasksToCleanupList = tasksToCleanupMap.get(instance)
              .stream().map(DatastreamTask::getDatastreamTaskName).collect(Collectors.toList());
          _log.warn("Tasks to cleanup for connector {} on instance {} : {}", connectorType, instance, tasksToCleanupList);
        }
        if (_config.getPerformPreAssignmentCleanup()) {
          _adapter.removeTaskNodes(tasksToCleanupMap);
        }
      }

    }

    _log.info("performPreAssignmentCleanup: completed");
  }

  /**
   * assign the partition to tasks for a particular datastreamGroup
   *
   * @param datastreamGroupName the datastreamGroup that needs to perform the partition assignment
   */
  private void performPartitionAssignment(String datastreamGroupName) {
    boolean succeeded;
    _log.info("START: Coordinator::performPartitionAssignment.");
    Map<String, Set<DatastreamTask>> previousAssignmentByInstance = new HashMap<>();
    Map<String, List<DatastreamTask>> newAssignmentsByInstance = new HashMap<>();

    try {
      previousAssignmentByInstance = _adapter.getAllAssignedDatastreamTasks();
      Map<String, Set<DatastreamTask>> assignmentByInstance = new HashMap<>(previousAssignmentByInstance);

      // retrieve the datastreamGroups for validation
      DatastreamGroup toProcessDatastream =
          fetchDatastreamGroups().stream().filter(dg -> datastreamGroupName.equals(dg.getName())).findFirst().orElse(null);

      if (toProcessDatastream != null) {
        AssignmentStrategy strategy = _connectors.get(toProcessDatastream.getConnectorName()).getAssignmentStrategy();
        Connector connectorInstance = _connectors.get(toProcessDatastream.getConnectorName()).getConnector()
            .getConnectorInstance();
        Map<String, Optional<DatastreamGroupPartitionsMetadata>> datastreamPartitions =
            connectorInstance.getDatastreamPartitions();

        if (datastreamPartitions.containsKey(toProcessDatastream.getName())) {
          DatastreamGroupPartitionsMetadata subscribes = connectorInstance.getDatastreamPartitions()
              .get(toProcessDatastream.getName())
              .orElseThrow(() ->
                  new DatastreamTransientException("Subscribed partition is not ready yet for datastream " +
                      toProcessDatastream.getName()));

          assignmentByInstance = strategy.assignPartitions(assignmentByInstance, subscribes);
        } else {
          // The datastream group will not found only when the datastream was just paused/removed but we happened to
          // handle the scheduled LEADER_PARTITION_EVENT. In either case we should just ignore and don't retry.
          _log.warn("partitions for {} is not found, ignore the partition assignment", toProcessDatastream.getName());
        }
      } else {
        _log.warn("datastream group {} is not active, ignore the partition assignment", datastreamGroupName);
      }

      for (String key : assignmentByInstance.keySet()) {
        newAssignmentsByInstance.put(key, new ArrayList<>(assignmentByInstance.get(key)));
      }
      _adapter.updateAllAssignments(newAssignmentsByInstance);
      _log.info("Partition assignment completed: datastreamGroup {}, assignment {} ", datastreamGroupName,
          assignmentByInstance);
      succeeded = true;
    } catch (Exception ex) {
      _log.warn("Partition assignment failed, Exception: ", ex);
      succeeded = false;
    }
    // schedule retry if failure
    if (succeeded) {
      _adapter.cleanUpOldUnusedTasksFromConnector(previousAssignmentByInstance, newAssignmentsByInstance);
      updateCounterForMaxPartitionInTask(newAssignmentsByInstance);
      _metrics.updateMeter(CoordinatorMetrics.Meter.NUM_PARTITION_ASSIGNMENTS, 1);
    } else {
      _metrics.updateKeyedMeter(CoordinatorMetrics.KeyedMeter.HANDLE_LEADER_PARTITION_ASSIGNMENT_NUM_RETRIES, 1);
      _scheduledExecutor.schedule(() -> {
        _log.warn("Retry scheduled for leader partition assignment, dg {}", datastreamGroupName);
        // We need to schedule both LEADER_DO_ASSIGNMENT and leader partition assignment in case the tasks are
        // not locked because the assigned instance is dead. As we use a sticky assignment, the leader do assignment
        // shouldn't generate too much extra costs
        _eventQueue.put(CoordinatorEvent.createLeaderDoAssignmentEvent(false));
        _eventQueue.put(CoordinatorEvent.createLeaderPartitionAssignmentEvent(datastreamGroupName));
      }, _config.getRetryIntervalMs(), TimeUnit.MILLISECONDS);
    }
    _log.info("END: Coordinator::performPartitionAssignment.");
  }

  private void updateCounterForMaxPartitionInTask(Map<String, List<DatastreamTask>> assignments) {
    long maxPartitionCount = 0;
    for (List<DatastreamTask> tasks : assignments.values()) {
      maxPartitionCount = Math.max(maxPartitionCount,
          tasks.stream().map(DatastreamTask::getPartitionsV2).map(List::size).mapToInt(v -> v).max().orElse(0));
    }
    _log.info("Max partition count assigned in the task {}", maxPartitionCount);
    MAX_PARTITION_COUNT.getAndSet(maxPartitionCount);
  }


  private void onDatastreamChange(List<DatastreamGroup> datastreamGroups) {
    //We need to perform handleDatastream only on active datastreams for partition listening
    List<DatastreamGroup> activeDataStreams = datastreamGroups.stream().filter(dg -> !dg.isPaused()).collect(Collectors.toList());

    for (String connectorType : _connectors.keySet()) {
      ConnectorWrapper connectorWrapper = _connectors.get(connectorType).getConnector();

      List<DatastreamGroup> datastreamsPerConnectorType = activeDataStreams.stream()
          .filter(x -> x.getConnectorName().equals(connectorType))
          .collect(Collectors.toList());

      connectorWrapper.getConnectorInstance().handleDatastream(datastreamsPerConnectorType);
    }
  }

  /**
   * move the partitions based on targetAssignmentInfo stored in the Zookeeper
   * @param notifyTimestamp the timestamp when partition movement is triggered
   */
  private void performPartitionMovement(Long notifyTimestamp) {
    _log.info("START: Coordinator::performPartitionMovement.");
    boolean shouldRetry = true;
    Map<String, Set<DatastreamTask>> previousAssignmentByInstance = _adapter.getAllAssignedDatastreamTasks();
    Map<String, List<DatastreamTask>> newAssignmentsByInstance = new HashMap<>();

    try {
      Map<String, Set<DatastreamTask>> assignmentByInstance = new HashMap<>(previousAssignmentByInstance);
      List<DatastreamGroup> toCleanUp = new ArrayList<>();

      for (String connectorType : _connectors.keySet()) {
        AssignmentStrategy strategy = _connectors.get(connectorType).getAssignmentStrategy();
        Connector connectorInstance = _connectors.get(connectorType).getConnector().getConnectorInstance();

        // Get the partition assignment information
        Map<String, Optional<DatastreamGroupPartitionsMetadata>> datastreamPartitions =
            connectorInstance.getDatastreamPartitions();

        // Get the datastream Group name which have the target assignment
        List<String> toMoveDatastream = _adapter.getDatastreamsNeedPartitionMovement(connectorType);

        // Fetch all live datastreamGroups
        List<DatastreamGroup> liveDatastreamGroups =
            fetchDatastreamGroups().stream().filter(group1 -> connectorType.equals(group1.getConnectorName())).collect(
                Collectors.toList());

        // clean up the datastreams if they are not in the live datastreams
        toMoveDatastream.stream().filter(dgName -> !liveDatastreamGroups.stream().map(DatastreamGroup::getName)
            .collect(Collectors.toList()).contains(dgName)).forEach(obsoleteDs ->
            _adapter.cleanUpPartitionMovement(connectorType, obsoleteDs, notifyTimestamp));

        // Filtered all live datastreamGroup as we process only datastream which have
        // both partition assignment info and the target assignment
        List<DatastreamGroup> toProcessedDatastreamGroups =
            liveDatastreamGroups.stream().filter(group2 -> toMoveDatastream.contains(group2.getName()))
                .filter(group3 -> datastreamPartitions.containsKey(group3.getName()))
                .collect(Collectors.toList());

        for (DatastreamGroup dg : toProcessedDatastreamGroups) {
          // Right now we fails the entire partition movement if any failure is encountered in any datastreamGroup
          // The behavior can be improved to enhance the isolation in partition movement from different datastreamGroups
          DatastreamGroupPartitionsMetadata subscribedPartitions = connectorInstance.getDatastreamPartitions().get(dg.getName())
              .orElseThrow(() -> new DatastreamTransientException("partition listener is not ready yet for datastream " + dg.getName()));
          Map<String, Set<String>> suggestedAssignment =
              _adapter.getPartitionMovement(dg.getConnectorName(), dg.getName(), notifyTimestamp);
          assignmentByInstance = strategy.movePartitions(assignmentByInstance, suggestedAssignment,
              subscribedPartitions);
          toCleanUp.add(dg);
        }
      }

      for (String key : assignmentByInstance.keySet()) {
        newAssignmentsByInstance.put(key, new ArrayList<>(assignmentByInstance.get(key)));
      }

      _adapter.updateAllAssignments(newAssignmentsByInstance);

      //clean up stored target assignment after the assignment is updated
      for (DatastreamGroup dg : toCleanUp) {
        _adapter.cleanUpPartitionMovement(dg.getConnectorName(), dg.getName(), notifyTimestamp);
      }

      _log.info("Partition movement completed: datastreamGroup, assignment {} ", assignmentByInstance);

      shouldRetry = false;
    } catch (DatastreamTransientException ex) {
      _log.warn("Partition movement failed, retry again after a configurable period", ex);
      shouldRetry = true;
    } catch (Exception ex) {
      // We do not retry if it is not transient exception. Unfortunately we don't have a good way to communicate to the
      // caller about individual failure as partition movement is an async process. A caller could only verify if the
      // request is completed by query the assignment

      _log.error("Partition movement failed, Exception: ", ex);
      _metrics.updateKeyedMeter(CoordinatorMetrics.KeyedMeter.HANDLE_LEADER_PARTITION_MOVEMENT_NUM_ERRORS, 1);
    }
    if (!shouldRetry) {
      _adapter.cleanUpOldUnusedTasksFromConnector(previousAssignmentByInstance, newAssignmentsByInstance);
      updateCounterForMaxPartitionInTask(newAssignmentsByInstance);
      _metrics.updateMeter(CoordinatorMetrics.Meter.NUM_PARTITION_MOVEMENTS, 1);
    }  else {
      _log.info("Schedule retry for leader movement tasks");
      _metrics.updateKeyedMeter(CoordinatorMetrics.KeyedMeter.HANDLE_LEADER_PARTITION_MOVEMENT_NUM_RETRIES, 1);
      _scheduledExecutor.schedule(() ->
          _eventQueue.put(CoordinatorEvent.createPartitionMovementEvent(notifyTimestamp)), _config.getRetryIntervalMs(),
          TimeUnit.MILLISECONDS);
    }
    _log.info("END: Coordinator::performPartitionMovement.");
  }

  @VisibleForTesting
  void validateNewAssignment(Map<String, List<DatastreamTask>> newAssignmentsByInstance) {
    if (_config.getMaxDatastreamTasksPerInstance() > 0) {
      // If the cluster is configured to limit the max tasks per instance, check if any instances have a higher
      // number of tasks than expected, and fail the leader assignment on violation of this limit. This can be useful
      // to prevent other issues such as OOMs due to high memory usage which may be seen if we exceed the supportable
      // number of tasks per instance.
      Map<String, Integer> instancesWithTaskCountAboveThreshold = newAssignmentsByInstance.entrySet().stream()
          .filter(e -> !e.getKey().equals(PAUSED_INSTANCE) && (e.getValue().size() > _config.getMaxDatastreamTasksPerInstance()))
          .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().size()));
      if (instancesWithTaskCountAboveThreshold.size() > 0) {
        throw new DatastreamRuntimeException(String.format("Too many tasks assigned to some instances, max tasks per "
                + "instance: %d, instances above the threshold: %s", _config.getMaxDatastreamTasksPerInstance(),
            instancesWithTaskCountAboveThreshold));
      }
    }
  }

  private Map<String, List<DatastreamTask>> performAssignment(List<String> liveInstances,
      Map<String, Set<DatastreamTask>> previousAssignmentByInstance, List<DatastreamGroup> datastreamGroups) {
    Map<String, List<DatastreamTask>> newAssignmentsByInstance = new HashMap<>();

    _log.info("handleLeaderDoAssignment: start");
    _log.debug("handleLeaderDoAssignment: assignment before re-balancing: " + previousAssignmentByInstance);

    Set<DatastreamGroup> pausedDatastreamGroups =
        datastreamGroups.stream().filter(DatastreamGroup::isPaused).collect(Collectors.toSet());

    PAUSED_DATASTREAMS_GROUPS.set(pausedDatastreamGroups.size());

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

    validateNewAssignment(newAssignmentsByInstance);

    return newAssignmentsByInstance;
  }

  void performCleanupOrphanNodes() {
    _log.info("performCleanupOrphanNodes called");
    int orphanCount = _adapter.cleanUpOrphanConnectorTasks(_config.getZkCleanUpOrphanConnectorTask());
    _metrics.updateMeter(CoordinatorMetrics.Meter.NUM_ORPHAN_CONNECTOR_TASKS, orphanCount);
    int orphanLockCount = _adapter.cleanUpOrphanConnectorTaskLocks(_config.getZkCleanUpOrphanConnectorTaskLock());
    _metrics.updateMeter(CoordinatorMetrics.Meter.NUM_ORPHAN_CONNECTOR_TASK_LOCKS, orphanLockCount);
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
   *
   */
  public void addConnector(String connectorName, Connector connector, AssignmentStrategy strategy,
      boolean customCheckpointing, DatastreamDeduper deduper, String authorizerName) {
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

    connector.onPartitionChange(datastreamGroup ->
      _eventQueue.put(CoordinatorEvent.createLeaderPartitionAssignmentEvent(datastreamGroup.getName()))
    );

    ConnectorInfo connectorInfo =
        new ConnectorInfo(connectorName, connector, strategy, customCheckpointing, _cpProvider, deduper, authorizerName);
    _connectors.put(connectorName, connectorInfo);

    _metrics.addConnectorMetrics(connectorInfo);
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
      _metrics.updateKeyedMeter(CoordinatorMetrics.KeyedMeter.VALIDATE_DATASTREAMS_UPDATE_NUM_ERRORS, 1);
      throw e;
    }
  }

  /**
   * Validate the partition is managed by connector for this datastream
   * @param datastream datastream which needs the verification
   * @throws DatastreamValidationException if partition assignment is not supported
   */
  public void validatePartitionAssignmentSupported(Datastream datastream) throws DatastreamValidationException {
    try {
      String connectorName = datastream.getConnectorName();
      ConnectorInfo connectorInfo = _connectors.get(connectorName);
      if (connectorInfo == null) {
        throw new DatastreamValidationException("Invalid connector: " + connectorName);
      }

      Connector connectorInstance = connectorInfo.getConnector().getConnectorInstance();

      if (!connectorInstance.isPartitionManagementSupported()) {
        String msg = String.format("Partition assignment is not managed by connector, datastream %s",
            datastream.getName());
        _log.error(msg);
        throw new DatastreamValidationException(msg);
      }

    } catch (Exception e) {
      _metrics.updateKeyedMeter(CoordinatorMetrics.KeyedMeter.IS_PARTITION_ASSIGNMENT_SUPPORTED_NUM_ERRORS, 1);
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
      _metrics.updateKeyedMeter(CoordinatorMetrics.KeyedMeter.IS_DATASTREAM_UPDATE_TYPE_SUPPORTED_NUM_ERRORS, 1);
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
        String principal = Objects.requireNonNull(datastream.getMetadata()).get(DatastreamMetadataConstants.OWNER_KEY);

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
      _metrics.updateKeyedMeter(CoordinatorMetrics.KeyedMeter.INITIALIZE_DATASTREAM_NUM_ERRORS, 1);
      throw e;
    }

    Objects.requireNonNull(datastream.getMetadata()).putIfAbsent(CREATION_MS, String.valueOf(Instant.now().toEpochMilli()));
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
              ds -> Objects.requireNonNull(ds.getDestination())
                  .getConnectionString().equals(Objects.requireNonNull(datastream.getDestination()).getConnectionString()))
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
      if (!Objects.requireNonNull(datastream.getMetadata()).containsKey(DatastreamMetadataConstants.TASK_PREFIX)) {
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
    Objects.requireNonNull(existingStream.getMetadata()).entrySet().stream()
        .filter(e -> e.getKey().startsWith(SYSTEM_DESTINATION_PREFIX))
        .forEach(e -> Objects.requireNonNull(datastream.getMetadata()).put(e.getKey(), e.getValue()));

    // If the existing datastream group is paused, also pause this datastream.
    // This is to avoid the creation of a datastream to RESUME event production.
    if (existingStream.getStatus().equals(DatastreamStatus.PAUSED)) {
      datastream.setStatus(DatastreamStatus.PAUSED);
    }

    Objects.requireNonNull(datastream.getMetadata())
        .put(DatastreamMetadataConstants.TASK_PREFIX,
            existingStream.getMetadata().get(DatastreamMetadataConstants.TASK_PREFIX));
  }

  @Override
  public List<BrooklinMetricInfo> getMetricInfos() {
    return _metrics.getMetricInfos();
  }

  /**
   * Get the datastream clusterName
   */
  public String getClusterName() {
    return _clusterName;
  }

  @VisibleForTesting
  CoordinatorEventProcessor getEventThread() {
    return _eventThread;
  }


  /**
   * Add a transport provider that the coordinator can assign to datastreams it creates.
   * @param transportProviderName Name of transport provider.
   * @param admin Instance of TransportProviderAdmin that the coordinator can assign.
   */
  public void addTransportProvider(String transportProviderName, TransportProviderAdmin admin) {
    _transportProviderAdmins.put(transportProviderName, admin);
    _metrics.addMetricInfos(admin);
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
      _metrics.addMetricInfos((MetricsAware) authorizer);
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

  @VisibleForTesting
  CachedDatastreamReader getDatastreamCache() {
    return _datastreamCache;
  }

  private class CoordinatorEventProcessor extends Thread {
    @Override
    public void run() {
      _log.info("START CoordinatorEventProcessor thread");
      while (!_shutdown && !isInterrupted()) {
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

  @VisibleForTesting
  ZkAdapter getZkAdapter() {
    return _adapter;
  }

  @VisibleForTesting
  CoordinatorConfig getConfig() {
    return _config;
  }

  @VisibleForTesting
  static String getNumThroughputViolatingTopicsMetricName() {
    return CoordinatorMetrics.NUM_THROUGHPUT_VIOLATING_TOPICS_PER_DATASTREAM;
  }

  /**
   * Encapsulates metric registration and update for the {@link Coordinator}
   */
  private static class CoordinatorMetrics {
    private static final String MODULE = Coordinator.class.getSimpleName();

    private static final String NUM_RETRIES = "numRetries";
    private static final String NUM_ERRORS = "numErrors";
    private static final String HANDLE_EVENT_PREFIX = "handleEvent";

    // Gauge metrics
    private static final String MAX_PARTITION_COUNT_IN_TASK = "maxPartitionCountInTask";
    private static final String NUM_PAUSED_DATASTREAMS_GROUPS = "numPausedDatastreamsGroups";
    private static final String IS_LEADER = "isLeader";
    private static final String ZK_SESSION_EXPIRED = "zkSessionExpired";
    public static final String NUM_THROUGHPUT_VIOLATING_TOPICS_PER_DATASTREAM = "numThroughputViolatingTopics";

    // Connector common metrics
    private static final String NUM_DATASTREAMS = "numDatastreams";
    private static final String NUM_DATASTREAM_TASKS = "numDatastreamTasks";

    private final Coordinator _coordinator;
    private final List<BrooklinMetricInfo> _metricInfos;
    private final DynamicMetricsManager _dynamicMetricsManager;

    public CoordinatorMetrics(Coordinator coordinator) {
      _coordinator = coordinator;
      _metricInfos = new ArrayList<>();
      _dynamicMetricsManager = DynamicMetricsManager.getInstance();

      addComponentMetricInfos();

      registerMeterMetrics();
      registerKeyedMeterMetrics();
      registerGaugeMetrics();
      registerCounterMetrics();
    }

    public void addMetricInfos(MetricsAware metricsAware) {
      Optional.ofNullable(metricsAware.getMetricInfos()).ifPresent(_metricInfos::addAll);
    }

    public void addConnectorMetrics(ConnectorInfo connectorInfo) {
      addMetricInfos(connectorInfo.getConnector().getConnectorInstance());

      // Register common connector metrics
      // Use connector name for the metrics, as there can be multiple connectors specified in the config that use
      // same connector class.
      String connectorName = connectorInfo.getConnectorType();

      _dynamicMetricsManager.registerGauge(connectorName, NUM_DATASTREAMS,
          () -> connectorInfo.getConnector().getNumDatastreams());
      _metricInfos.add(new BrooklinGaugeInfo(MetricRegistry.name(connectorName, NUM_DATASTREAMS)));

      _dynamicMetricsManager.registerGauge(connectorName, NUM_DATASTREAM_TASKS,
          () -> connectorInfo.getConnector().getNumDatastreamTasks());
      _metricInfos.add(new BrooklinGaugeInfo(MetricRegistry.name(connectorName, NUM_DATASTREAM_TASKS)));

      AssignmentStrategy strategy = connectorInfo.getAssignmentStrategy();
      if (strategy instanceof MetricsAware) {
        addMetricInfos((MetricsAware) strategy);
      }
    }

    public List<BrooklinMetricInfo> getMetricInfos() {
      return Collections.unmodifiableList(_metricInfos);
    }

    public void updateMeter(Meter metric, int value) {
      _dynamicMetricsManager.createOrUpdateMeter(MODULE, metric.getName(), value);
    }

    public void updateKeyedMeter(KeyedMeter metric, int value) {
      _dynamicMetricsManager.createOrUpdateMeter(MODULE, metric.getKey(), metric.getName(), value);
    }

    public void updateCounter(Counter metric, int value) {
      _dynamicMetricsManager.createOrUpdateCounter(MODULE, metric.getName(), value);
    }

    public static KeyedMeter getKeyedMeter(EventType eventType) {
      switch (eventType) {
        case LEADER_DO_ASSIGNMENT:
          return KeyedMeter.LEADER_DO_ASSIGNMENT_NUM_ERRORS;
        case LEADER_PARTITION_ASSIGNMENT:
          return KeyedMeter.LEADER_PARTITION_ASSIGNMENT_NUM_ERRORS;
        case LEADER_PARTITION_MOVEMENT:
          return KeyedMeter.LEADER_PARTITION_MOVEMENT_NUM_ERRORS;
        case HANDLE_ASSIGNMENT_CHANGE:
          return KeyedMeter.HANDLE_ASSIGNMENT_CHANGE_NUM_ERRORS;
        case HANDLE_DATASTREAM_CHANGE_WITH_UPDATE:
          return KeyedMeter.HANDLE_DATASTREAM_CHANGE_WITH_UPDATE_NUM_ERRORS;
        case HANDLE_ADD_OR_DELETE_DATASTREAM:
          return KeyedMeter.HANDLE_ADD_OR_DELETE_DATASTREAM_NUM_ERRORS;
        case HANDLE_INSTANCE_ERROR:
          return KeyedMeter.HANDLE_INSTANCE_ERROR_NUM_ERRORS;
        case HEARTBEAT:
          return KeyedMeter.HEARTBEAT_NUM_ERRORS;
        case NO_OP:
          return KeyedMeter.NO_OP_NUM_ERRORS;
        default:
          throw new IllegalArgumentException("Unexpected Coordinator event type: " + eventType);
      }
    }

    private void addComponentMetricInfos() {
      // CheckpointProvider metrics
      addMetricInfos(_coordinator._cpProvider);

      // EventProducer metrics
      _metricInfos.addAll(EventProducer.getMetricInfos());
    }

    private void registerMeterMetrics() {
      // These metrics are eagerly created (i.e. they are registered with _dynamicMetricsManager
      // even before they are ever updated + we use BrooklinMeterInfos that specify metrics by full name)
      Arrays.stream(Meter.values()).forEach(this::registerMeter);
    }

    private void registerKeyedMeterMetrics() {
      // Our intent is to create KeyedMeter metrics lazily (i.e. only create the JMX metrics
      // when the metrics are actually updated), unless their isEagerlyRegister flag is set to true.
      //
      // To accomplish creating metrics lazily, we:
      //   - Refrain from registering metrics with _dynamicMetricsManager,
      //     and rely on createOrUpdate*() methods so they are created upon
      //     update instead.
      //   - Return regex-based BrooklinMeterInfo as opposed to ones that
      //     specify metrics by their full names

      // We register some KeyedMeter metrics eagerly
      for (KeyedMeter keyedMeter : KeyedMeter.values()) {
        if (keyedMeter.isEagerlyRegistered()) {
          _dynamicMetricsManager.registerMetric(MODULE, keyedMeter.getKey(), keyedMeter.getName(),
              com.codahale.metrics.Meter.class);
        }
      }

      // All KeyedMeter metrics are covered by these two regex-based BrooklinMeterInfo objects
      String prefix = _coordinator.getDynamicMetricPrefixRegex();
      _metricInfos.add(new BrooklinMeterInfo(prefix + NUM_ERRORS));
      _metricInfos.add(new BrooklinMeterInfo(prefix + NUM_RETRIES));
    }

    private void registerGaugeMetrics() {
      // Gauges must be eagerly created
      ImmutableMap<String, Supplier<?>> gaugeMetrics = ImmutableMap.<String, Supplier<?>>builder()
          .put(MAX_PARTITION_COUNT_IN_TASK, MAX_PARTITION_COUNT::get)
          .put(NUM_PAUSED_DATASTREAMS_GROUPS, PAUSED_DATASTREAMS_GROUPS::get)
          .put(IS_LEADER, () -> _coordinator.getIsLeader().getAsBoolean() ? 1 : 0)
          .put(ZK_SESSION_EXPIRED, () -> _coordinator.isZkSessionExpired() ? 1 : 0)
          .build();
      gaugeMetrics.forEach(this::registerGauge);

      // For dynamic datastream prefixed gauge metric reporting num throughput violating topics
      _metricInfos.add(new BrooklinGaugeInfo(_coordinator.buildMetricName(MODULE,
          MetricsAware.KEY_REGEX + NUM_THROUGHPUT_VIOLATING_TOPICS_PER_DATASTREAM)));
    }

    private void registerCounterMetrics() {
      // These metrics are eagerly created
      Arrays.stream(Counter.values()).forEach(this::registerCounter);
    }

    private void registerMeter(Meter metric) {
      String metricName = metric.getName();
      _dynamicMetricsManager.registerMetric(MODULE, metricName, com.codahale.metrics.Meter.class);
      _metricInfos.add(new BrooklinMeterInfo(_coordinator.buildMetricName(MODULE, metricName)));
    }

    private void registerGauge(String metricName, Supplier<?> valueSupplier) {
      _dynamicMetricsManager.registerGauge(MODULE, metricName, valueSupplier);
      _metricInfos.add(new BrooklinGaugeInfo(_coordinator.buildMetricName(MODULE, metricName)));
    }

    // registers a new gauge or updates the supplier for the gauge if it already exists
    private <T> void registerOrSetKeyedGauge(String key, String metricName, Supplier<T> valueSupplier) {
      _dynamicMetricsManager.setGauge(_dynamicMetricsManager.registerGauge(MODULE, key, metricName, valueSupplier),
          valueSupplier);
    }

    private void registerCounter(Counter metric) {
      String metricName = metric.getName();
      _dynamicMetricsManager.registerMetric(MODULE, metricName, com.codahale.metrics.Counter.class);
      _metricInfos.add(new BrooklinCounterInfo(_coordinator.buildMetricName(MODULE, metricName)));
    }

    /**
     * Coordinator metrics of type {@link com.codahale.metrics.Meter}
     */
    public enum Meter {
      NUM_REBALANCES("numRebalances"),
      NUM_FAILED_STOPS("numFailedStops"),
      NUM_INFERRED_STOPPING_STREAMS("numInferredStoppingStreams"),
      NUM_ASSIGNMENT_CHANGES("numAssignmentChanges"),
      NUM_PARTITION_ASSIGNMENTS("numPartitionAssignments"),
      NUM_PARTITION_MOVEMENTS("numPartitionMovements"),
      NUM_ORPHAN_CONNECTOR_TASKS("numOrphanConnectorTasks"),
      NUM_ORPHAN_CONNECTOR_TASK_LOCKS("numOrphanConnectorTaskLocks");

      private final String _name;

      Meter(String name) {
        _name = name;
      }

      public String getName() {
        return _name;
      }
    }

    /**
     * Keyed Coordinator metrics of type {@link com.codahale.metrics.Meter}
     */
    public enum KeyedMeter {
      HANDLE_DATASTREAM_ADD_OR_DELETE_NUM_RETRIES("handleDatastreamAddOrDelete", NUM_RETRIES),
      HANDLE_LEADER_DO_ASSIGNMENT_NUM_RETRIES("handleLeaderDoAssignment", NUM_RETRIES, true),
      HANDLE_LEADER_PARTITION_ASSIGNMENT_NUM_RETRIES("handleLeaderPartitionAssignment", NUM_RETRIES, true),
      HANDLE_LEADER_PARTITION_MOVEMENT_NUM_ERRORS("handleLeaderPartitionMovement", NUM_ERRORS),
      HANDLE_LEADER_PARTITION_MOVEMENT_NUM_RETRIES("handleLeaderPartitionMovement", NUM_RETRIES),
      VALIDATE_DATASTREAMS_UPDATE_NUM_ERRORS("validateDatastreamsUpdate", NUM_ERRORS),
      IS_PARTITION_ASSIGNMENT_SUPPORTED_NUM_ERRORS("isPartitionAssignmentSupported", NUM_ERRORS),
      IS_DATASTREAM_UPDATE_TYPE_SUPPORTED_NUM_ERRORS("isDatastreamUpdateTypeSupported", NUM_ERRORS),
      INITIALIZE_DATASTREAM_NUM_ERRORS("initializeDatastream", NUM_ERRORS),
      POST_DATASTREAMS_STATE_CHANGE_ACTION_NUM_ERRORS("postDatastreamStateChangeAction", NUM_ERRORS),
      /* Coordinator event metrics */
      LEADER_DO_ASSIGNMENT_NUM_ERRORS(HANDLE_EVENT_PREFIX + EventType.LEADER_DO_ASSIGNMENT, NUM_ERRORS),
      LEADER_PARTITION_ASSIGNMENT_NUM_ERRORS(HANDLE_EVENT_PREFIX + EventType.LEADER_PARTITION_ASSIGNMENT, NUM_ERRORS),
      LEADER_PARTITION_MOVEMENT_NUM_ERRORS(HANDLE_EVENT_PREFIX + EventType.LEADER_PARTITION_MOVEMENT, NUM_ERRORS),
      HANDLE_ASSIGNMENT_CHANGE_NUM_ERRORS(HANDLE_EVENT_PREFIX + EventType.HANDLE_ASSIGNMENT_CHANGE, NUM_ERRORS),
      HANDLE_DATASTREAM_CHANGE_WITH_UPDATE_NUM_ERRORS(HANDLE_EVENT_PREFIX + HANDLE_DATASTREAM_CHANGE_WITH_UPDATE, NUM_ERRORS),
      HANDLE_ADD_OR_DELETE_DATASTREAM_NUM_ERRORS(HANDLE_EVENT_PREFIX + EventType.HANDLE_ADD_OR_DELETE_DATASTREAM, NUM_ERRORS),
      HANDLE_INSTANCE_ERROR_NUM_ERRORS(HANDLE_EVENT_PREFIX + EventType.HANDLE_INSTANCE_ERROR, NUM_ERRORS),
      HEARTBEAT_NUM_ERRORS(HANDLE_EVENT_PREFIX + EventType.HEARTBEAT, NUM_ERRORS),
      NO_OP_NUM_ERRORS(HANDLE_EVENT_PREFIX + EventType.NO_OP, NUM_ERRORS);

      private final String _key;
      private final String _name;
      private final boolean _isEagerlyRegistered;

      KeyedMeter(String key, String name) {
        this(key, name, false);
      }

      KeyedMeter(String key, String name, boolean isEagerlyRegistered) {
        _key = key;
        _name = name;
        _isEagerlyRegistered = isEagerlyRegistered;
      }

      public String getKey() {
        return _key;
      }

      public String getName() {
        return _name;
      }

      public boolean isEagerlyRegistered() {
        return _isEagerlyRegistered;
      }
    }

    /**
     * Coordinator metrics of type {@link com.codahale.metrics.Counter}
     */
    public enum Counter {
      NUM_HEARTBEATS("numHeartbeats");

      private final String _name;

      Counter(String name) {
        _name = name;
      }

      public String getName() {
        return _name;
      }
    }
  }
}
