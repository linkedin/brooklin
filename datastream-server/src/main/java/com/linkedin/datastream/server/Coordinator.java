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

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.Validate;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;

import com.linkedin.datastream.common.Datastream;
import com.linkedin.datastream.common.DatastreamDestination;
import com.linkedin.datastream.common.DatastreamException;
import com.linkedin.datastream.common.DatastreamMetadataConstants;
import com.linkedin.datastream.common.DatastreamStatus;
import com.linkedin.datastream.common.DatastreamUtils;
import com.linkedin.datastream.common.ErrorLogger;
import com.linkedin.datastream.common.VerifiableProperties;
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
import com.linkedin.datastream.server.api.serde.SerdeAdmin;
import com.linkedin.datastream.server.api.strategy.AssignmentStrategy;
import com.linkedin.datastream.server.api.transport.TransportException;
import com.linkedin.datastream.server.api.transport.TransportProvider;
import com.linkedin.datastream.server.api.transport.TransportProviderAdmin;
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
  private final Properties _eventProducerConfig;
  private final CheckpointProvider _cpProvider;

  private final Map<String, TransportProviderAdmin> _transportProviderAdmins = new HashMap<>();
  private Logger _log = LoggerFactory.getLogger(Coordinator.class.getName());

  private static final long EVENT_THREAD_JOIN_TIMEOUT = 1000L;
  public static final String EVENT_PRODUCER_CONFIG_DOMAIN = "brooklin.server.eventProducer";

  private final CoordinatorEventBlockingQueue _eventQueue;
  private final CoordinatorEventProcessor _eventThread;
  private final ThreadPoolExecutor _assignmentChangeThreadPool;
  private final String _clusterName;
  private ScheduledExecutorService _executor = Executors.newSingleThreadScheduledExecutor();

  // make sure the scheduled retries are not duplicated
  AtomicBoolean leaderDatastreamAddOrDeleteEventScheduled = new AtomicBoolean(false);

  // make sure the scheduled retries are not duplicated
  AtomicBoolean leaderDoAssignmentScheduled = new AtomicBoolean(false);

  private final CoordinatorConfig _config;
  private final ZkAdapter _adapter;

  // mapping from connector type to connector Info instance
  private final Map<String, ConnectorInfo> _connectors = new HashMap<>();

  // Currently assigned datastream tasks by taskName
  private Map<String, DatastreamTask> _assignedDatastreamTasks = new HashMap<>();

  private final List<BrooklinMetricInfo> _metrics = new ArrayList<>();
  private final DynamicMetricsManager _dynamicMetricsManager;
  private static final String NUM_REBALANCES = "numRebalances";
  private static final String NUM_ERRORS = "numErrors";
  private static final String NUM_RETRIES = "numRetries";

  // Connector common metrics
  private static final String NUM_DATASTREAMS = "numDatastreams";
  private static final String NUM_DATASTREAM_TASKS = "numDatastreamTasks";
  private HashMap<String, SerdeAdmin> _serdeAdmins = new HashMap<>();

  public Coordinator(CachedDatastreamReader datastreamCache, Properties config) throws DatastreamException {
    this(datastreamCache, new CoordinatorConfig(config));
  }

  public Coordinator(CachedDatastreamReader datastreamCache, CoordinatorConfig config) throws DatastreamException {
    _datastreamCache = datastreamCache;
    _config = config;
    _clusterName = _config.getCluster();

    _adapter = new ZkAdapter(_config.getZkAddress(), _clusterName, _config.getZkSessionTimeout(),
        _config.getZkConnectionTimeout(), this);

    _eventQueue = new CoordinatorEventBlockingQueue();
    _eventThread = new CoordinatorEventProcessor();
    _eventThread.setDaemon(true);

    _dynamicMetricsManager = DynamicMetricsManager.getInstance();

    // Creating a separate thread pool for making the onAssignmentChange calls to the connector
    _assignmentChangeThreadPool = new ThreadPoolExecutor(config.getAssignmentChangeThreadPoolThreadCount(),
        config.getAssignmentChangeThreadPoolThreadCount(), 10, TimeUnit.SECONDS, new LinkedBlockingQueue<>());

    VerifiableProperties coordinatorProperties = new VerifiableProperties(_config.getConfigProperties());

    _eventProducerConfig = coordinatorProperties.getDomainProperties(EVENT_PRODUCER_CONFIG_DOMAIN);

    _cpProvider = new ZookeeperCheckpointProvider(_adapter);
    Optional.ofNullable(_cpProvider.getMetricInfos()).ifPresent(_metrics::addAll);

    _metrics.addAll(EventProducer.getMetricInfos());
  }

  public void start() {
    _log.info("Starting coordinator");
    _eventThread.start();
    _adapter.connect();
    _log = LoggerFactory.getLogger(String.format("%s:%s", Coordinator.class.getName(), _adapter.getInstanceName()));

    for (String connectorType : _connectors.keySet()) {
      ConnectorInfo connectorInfo = _connectors.get(connectorType);
      ConnectorWrapper connector = connectorInfo.getConnector();

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
      DatastreamTaskImpl task = _adapter.getAssignedDatastreamTask(_adapter.getInstanceName(), taskName);
      DatastreamGroup dg = _datastreamCache.getDatastreamGroups()
          .stream()
          .filter(x -> x.getTaskPrefix().equals(task.getTaskPrefix()))
          .findFirst()
          .get();

      task.setDatastreams(dg.getDatastreams());
      return task;
    }
  }

  private Future<Void> dispatchAssignmentChangeIfNeeded(String connectorType, List<DatastreamTask> assignment) {
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

    if (!addedTasks.isEmpty() || !removedTasks.isEmpty()) {
      // Populate the event producers before calling the connector with the list of tasks.
      addedTasks.stream().filter(t -> t.getEventProducer() == null).forEach(this::initializeTask);

      // Dispatch the onAssignmentChange to the connector in a separate thread.
      return _assignmentChangeThreadPool.submit((Callable<Void>) () -> {
        try {
          connector.onAssignmentChange(assignment);
          // Unassign tasks with producers
          removedTasks.forEach(this::uninitializeTask);
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

  private void uninitializeTask(DatastreamTask t) {
    TransportProviderAdmin tpAdmin = _transportProviderAdmins.get(t.getTransportProviderName());
    tpAdmin.unassignTransportProvider(t);
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
    List<Datastream> allStreams = _datastreamCache.getAllDatastreams(true);

    // do nothing if there are zero datastreams
    if (allStreams.isEmpty()) {
      _log.warn("Received a new datastream event, but there were no datastreams");
      return;
    }

    for (Datastream ds : allStreams) {
      if (ds.getStatus() == DatastreamStatus.INITIALIZING) {
        try {
          createTopic(ds);

          // Set the datastream status as ready for use (both producing and consumption)
          ds.setStatus(DatastreamStatus.READY);
          if (!_adapter.updateDatastream(ds)) {
            _log.warn(String.format("Failed to update datastream: %s after initializing, "
                + "This datastream will not be scheduled for producing events ", ds.getName()));
            shouldRetry = true;
          }
        } catch (Exception e) {
          _log.warn("Failed to update the destination of new datastream {}", ds, e);
          shouldRetry = true;
        }
      } else if (ds.getStatus() == DatastreamStatus.DELETING) {
        _log.info("Trying to hard delete datastream {}", ds.getName());
        hardDeleteDatastream(ds, allStreams);
      }
    }

    if (shouldRetry) {
      _dynamicMetricsManager.createOrUpdateMeter(this.getClass(), "handleDatastreamAddOrDelete", NUM_RETRIES, 1);

      // If there are any failure, we will need to schedule retry if
      // there is no pending retry scheduled already.
      // TODO(misanchez) This condition is always true beacuse we are running in syncronized mode.
      //                 we should consider to remove this AtomicBoolean.
      if (leaderDatastreamAddOrDeleteEventScheduled.compareAndSet(false, true)) {
        _log.warn("Schedule retry for handling new datastream");
        _executor.schedule(() -> _eventQueue.put(CoordinatorEvent.createHandleDatastreamAddOrDeleteEvent()),
            _config.getRetryIntervalMS(), TimeUnit.MILLISECONDS);
      }
    }

    _eventQueue.put(CoordinatorEvent.createLeaderDoAssignmentEvent());
  }

  private void hardDeleteDatastream(Datastream ds, List<Datastream> allStreams) {
    String taskPrefix = DatastreamUtils.getTaskPrefix(ds);
    Optional<Datastream> duplicateStream = allStreams.stream()
        .filter(x -> !x.getName().equals(ds.getName()) && DatastreamUtils.getTaskPrefix(x).equals(taskPrefix))
        .findFirst();

    if (!duplicateStream.isPresent()) {
      _log.info(
          "No datastream left in the datastream group with taskPrefix {}. Deleting all tasks corresponding to the datastream.",
          taskPrefix);
      _adapter.deleteTasksWithPrefix(_connectors.keySet(), taskPrefix);
    } else {
      _log.info("Found duplicate datastream {} for the datastream to be deleted {}. Not deleting the tasks.",
          duplicateStream.get().getName(), ds.getName());
    }

    _adapter.deleteDatastream(ds.getName());
  }

  private String createTopic(Datastream datastream) throws TransportException {
    Properties datastreamProperties = new Properties();
    if (datastream.hasMetadata()) {
      datastreamProperties.putAll(datastream.getMetadata());
    }

    _transportProviderAdmins.get(datastream.getTransportProviderName()).createDestination(datastream);

    // Set destination creation time and retention
    datastream.getMetadata()
        .put(DatastreamMetadataConstants.DESTINATION_CREATION_MS, String.valueOf(Instant.now().toEpochMilli()));

    try {
      Duration retention = _transportProviderAdmins.get(datastream.getTransportProviderName()).getRetention(datastream);
      if (retention != null) {
        datastream.getMetadata()
            .put(DatastreamMetadataConstants.DESTINATION_RETENION_MS, String.valueOf(retention.toMillis()));
      }
    } catch (UnsupportedOperationException e) {
      _log.warn("Transport doesn't support mechanism to get retention, Unable to populate retention in datastream", e);
    }

    return datastream.getDestination().getConnectionString();
  }

  private void handleLeaderDoAssignment() {

    // get all current live instances
    List<String> liveInstances = _adapter.getLiveInstances();

    // Get all streams that are assignable. Assignable datastreams are the ones:
    //  1) has a valid destination
    //  2) status is READY
    List<Datastream> allStreams = _datastreamCache.getAllDatastreams()
        .stream()
        .filter(datastream -> datastream.hasStatus() && datastream.getStatus() == DatastreamStatus.READY
            && datastream.hasDestination() && datastream.getDestination().hasConnectionString())
        .collect(Collectors.toList());

    Map<String, List<Datastream>> streamsByTaskPrefix =
        allStreams.stream().collect(Collectors.groupingBy(DatastreamUtils::getTaskPrefix, Collectors.toList()));

    List<DatastreamGroup> datastreamGroups = streamsByTaskPrefix.keySet()
        .stream()
        .map(x -> new DatastreamGroup(streamsByTaskPrefix.get(x)))
        .collect(Collectors.toList());

    _log.debug("handleLeaderDoAssignment: final datastreams for task assignment: %s", datastreamGroups);

    // Map between Instance and the tasks
    Map<String, List<DatastreamTask>> newAssignmentsByInstance = new HashMap<>();

    // Map between instance to tasks assigned to the instance.
    Map<String, Set<DatastreamTask>> previousAssignmentByInstance = _adapter.getAllAssignedDatastreamTasks();

    _log.info("handleLeaderDoAssignment: assignment before re-balancing: " + previousAssignmentByInstance);

    for (String connectorType : _connectors.keySet()) {
      AssignmentStrategy strategy = _connectors.get(connectorType).getAssignmentStrategy();
      List<DatastreamGroup> datastreamsPerConnectorType = datastreamGroups.stream()
          .filter(x -> x.getConnectorName().equals(connectorType))
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
          newAssignmentsByInstance.get(instance).add(task);
        });
      }
    }

    // persist the assigned result to zookeeper. This means we will need to compare with the current
    // assignment and do remove and add znodes accordingly. In the case of zookeeper failure (when
    // it failed to create or delete znodes), we will do our best to continue the current process
    // and schedule a retry. The retry should be able to diff the remaining zookeeper work
    boolean succeeded = true;
    try {
        _adapter.updateAllAssignments(newAssignmentsByInstance);
      _adapter.updateAllAssignments(newAssignmentsByInstance);
    } catch (RuntimeException e) {
      _log.error("handleLeaderDoAssignment: runtime Exception while updating Zookeeper.", e);
      succeeded = false;
    }

    _log.info("handleLeaderDoAssignment: new assignment: " + newAssignmentsByInstance);

    // clean up tasks under dead instances if everything went well
    if (succeeded) {
      _adapter.cleanupDeadInstanceAssignments(liveInstances);
      _adapter.cleanupOldUnusedTasks(previousAssignmentByInstance, newAssignmentsByInstance);
      _dynamicMetricsManager.createOrUpdateMeter(this.getClass(), NUM_REBALANCES, 1);
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
      boolean customCheckpointing, DatastreamDeduper deduper) {
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

    Optional<List<BrooklinMetricInfo>> connectorMetrics = Optional.ofNullable(connector.getMetricInfos());
    connectorMetrics.ifPresent(_metrics::addAll);

    ConnectorInfo connectorInfo = new ConnectorInfo(connectorName, connector, strategy, customCheckpointing, deduper);
    _connectors.put(connectorName, connectorInfo);

    // Register common connector metrics
    Class<?> connectorClass = connector.getClass();
    _dynamicMetricsManager.registerMetric(connectorClass, NUM_DATASTREAMS,
        (Gauge<Long>) () -> connectorInfo.getConnector().getNumDatastreams());
    _dynamicMetricsManager.registerMetric(connectorClass, NUM_DATASTREAM_TASKS,
        (Gauge<Long>) () -> connectorInfo.getConnector().getNumDatastreamTasks());

    String className = connectorClass.getSimpleName();
    _metrics.add(new BrooklinGaugeInfo(MetricRegistry.name(className, NUM_DATASTREAMS)));
    _metrics.add(new BrooklinGaugeInfo(MetricRegistry.name(className, NUM_DATASTREAM_TASKS)));
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
    ConnectorInfo connectorInfo = _connectors.get(connectorName);
    if (connectorInfo == null) {
      String errorMessage = "Invalid connector: " + connectorName;
      _log.error(errorMessage);
      throw new DatastreamValidationException(errorMessage);
    }

    ConnectorWrapper connector = connectorInfo.getConnector();
    DatastreamDeduper deduper = connectorInfo.getDatastreamDeduper();

    List<Datastream> allDatastreams = _datastreamCache.getAllDatastreams()
        .stream()
        .filter(d -> d.getConnectorName().equals(connectorName))
        .collect(Collectors.toList());

    if (!StringUtils.isEmpty(_config.getDefaultTransportProviderName())) {
      if (!datastream.hasTransportProviderName() || StringUtils.isEmpty(datastream.getTransportProviderName())) {
        datastream.setTransportProviderName(_config.getDefaultTransportProviderName());
      }
    }

    try {
      connector.initializeDatastream(datastream, allDatastreams);

      Optional<Datastream> existingDatastream = Optional.empty();

      // Find the duplicate datastream only when the destination is not populated.
      if (!datastream.hasDestination() || !datastream.getDestination().hasConnectionString() || StringUtils.isEmpty(
          datastream.getDestination().getConnectionString())) {
        existingDatastream = deduper.findExistingDatastream(datastream, allDatastreams);
      }

      if (existingDatastream.isPresent()) {
        populateDatastreamDestinationFromExistingDatastream(datastream, existingDatastream.get());
        datastream.getMetadata()
            .put(DatastreamMetadataConstants.TASK_PREFIX,
                existingDatastream.get().getMetadata().get(DatastreamMetadataConstants.TASK_PREFIX));
      } else {
        if (!_transportProviderAdmins.containsKey(datastream.getTransportProviderName())) {
          throw new DatastreamValidationException(
              String.format("Transport provider \"%s\" is undefined", datastream.getTransportProviderName()));
        }
        _transportProviderAdmins.get(datastream.getTransportProviderName())
            .initializeDestinationForDatastream(datastream);

        datastream.getMetadata()
            .put(DatastreamMetadataConstants.TASK_PREFIX, DatastreamTaskImpl.getTaskPrefix(datastream));

        _log.info("Datastream {} has an unique source or topicReuse is set to true, Assigning a new destination {}",
            datastream.getName(), datastream.getDestination());
      }
    } catch (Exception e) {
      _dynamicMetricsManager.createOrUpdateMeter(this.getClass(), "initializeDatastream", NUM_ERRORS, 1);
      throw e;
    }

    datastream.getMetadata().put(DatastreamMetadataConstants.CREATION_MS, String.valueOf(Instant.now().toEpochMilli()));
  }

  private void populateDatastreamDestinationFromExistingDatastream(Datastream datastream, Datastream existingStream) {
    DatastreamDestination destination = existingStream.getDestination();
    datastream.setDestination(destination);

    // Copy destination-related metadata
    if (existingStream.getMetadata().containsKey(DatastreamMetadataConstants.DESTINATION_CREATION_MS)) {
      datastream.getMetadata()
          .put(DatastreamMetadataConstants.DESTINATION_CREATION_MS,
              existingStream.getMetadata().get(DatastreamMetadataConstants.DESTINATION_CREATION_MS));
    }

    if (existingStream.getMetadata().containsKey(DatastreamMetadataConstants.DESTINATION_RETENION_MS)) {
      datastream.getMetadata()
          .put(DatastreamMetadataConstants.DESTINATION_RETENION_MS,
              existingStream.getMetadata().get(DatastreamMetadataConstants.DESTINATION_RETENION_MS));
    }
  }

  @Override
  public List<BrooklinMetricInfo> getMetricInfos() {
    _metrics.add(new BrooklinMeterInfo(buildMetricName(NUM_REBALANCES)));
    _metrics.add(new BrooklinMeterInfo(getDynamicMetricPrefixRegex() + NUM_ERRORS));
    _metrics.add(new BrooklinMeterInfo(getDynamicMetricPrefixRegex() + NUM_RETRIES));

    return Collections.unmodifiableList(_metrics);
  }

  /**
   * @return the datastream clusterName
   */
  public String getClusterName() {
    return _clusterName;
  }

  public void addTransportProvider(String transportProviderName, TransportProviderAdmin admin) {
    _transportProviderAdmins.put(transportProviderName, admin);

    Optional<List<BrooklinMetricInfo>> transportProviderMetrics = Optional.ofNullable(admin.getMetricInfos());
    transportProviderMetrics.ifPresent(_metrics::addAll);
  }

  public void addSerde(String serdeName, SerdeAdmin admin) {
    _serdeAdmins.put(serdeName, admin);
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
