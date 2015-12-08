package com.linkedin.datastream.server;

import java.util.ArrayList;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import com.linkedin.datastream.common.Datastream;
import com.linkedin.datastream.common.DatastreamDestination;
import com.linkedin.datastream.common.DatastreamException;
import com.linkedin.datastream.common.DatastreamTarget;
import com.linkedin.datastream.common.KafkaConnection;
import com.linkedin.datastream.common.ReflectionUtils;
import com.linkedin.datastream.server.zk.ZkAdapter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 *
 * Coordinator is the object that bridges the ZooKeeper with Connector implementations. There is one instance
 * of Coordinator for each deployable DatastreamService instance. The Cooridnator can connect multiple connectors,
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

public class Coordinator implements ZkAdapter.ZkAdapterListener {
  private static final Logger LOG = LoggerFactory.getLogger(Coordinator.class.getName());

  private static final long EVENT_THREAD_JOIN_TIMEOUT = 1000L;

  private final CoordinatorEventBlockingQueue _eventQueue;
  private final CoordinatorEventProcessor _eventThread;
  private final EventProducerPool _eventProducerPool;
  private ScheduledExecutorService _executor = Executors.newSingleThreadScheduledExecutor();

  // make sure the scheduled retries are not duplicated
  AtomicBoolean leaderDoAssignmentScheduled = new AtomicBoolean(false);

  private final CoordinatorConfig _config;
  private final ZkAdapter _adapter;

  // mapping from connector instance to associated assignment strategy
  private final Map<ConnectorWrapper, AssignmentStrategy> _strategies = new HashMap<>();

  // mapping from connector type to connector instance
  private final Map<String, ConnectorWrapper> _connectors = new HashMap<>();

  private final DatastreamEventCollectorFactory _eventCollectorFactory;

  private final TransportProvider _transportProvider;

  private final DestinationManager _destinationManager;

  // all datastreams by connector type. This is also valid for the coordinator leader
  // and it is stored after the leader finishes the datastream assignment
  private Map<String, List<DatastreamTask>> _allStreamsByConnectorType = new HashMap<>();

  public Coordinator(Properties config)
      throws DatastreamException {
    this(new CoordinatorConfig((config)));
  }

  public Coordinator(CoordinatorConfig config)
      throws DatastreamException {
    _config = config;
    _eventQueue = new CoordinatorEventBlockingQueue();
    _eventThread = new CoordinatorEventProcessor();
    _eventThread.setDaemon(true);

    _adapter = new ZkAdapter(_config.getZkAddress(), _config.getCluster(), _config.getZkSessionTimeout(),
        _config.getZkConnectionTimeout(), this);
    _adapter.setListener(this);
    _eventCollectorFactory = new DatastreamEventCollectorFactory(config.getConfigProperties());

    String transportFactory = config.getTransportProviderFactory();
    TransportProviderFactory factory = ReflectionUtils.createInstance(transportFactory);
    if (factory == null) {
      throw new DatastreamException("invalid transport provider factory: " + transportFactory);
    }
    _transportProvider = factory.createTransportProvider(_config.getConfigProperties());
    if (_transportProvider == null) {
      throw new DatastreamException("failed to create transport provider, factory: " + transportFactory);
    }

    _destinationManager = new DestinationManager(_transportProvider);
    _eventProducerPool = new EventProducerPool();
  }

  public void start() {
    _eventThread.start();
    _adapter.connect();

    _connectors.forEach((connectorType, connector) -> {
      // populate the instanceName. We only know the instance name after _adapter.connect()
        connector.setInstanceName(getInstanceName());

        // make sure connector znode exists upon instance start. This way in a brand new cluster
        // we can inspect zookeeper and know what connectors are created
        _adapter.ensureConnectorZNode(connector.getConnectorType());

        // call connector::start API
        connector.start(_eventCollectorFactory);
      });

    // now that instance is started, make sure it doesn't miss any assignment created during
    // the slow startup
    _eventQueue.put(CoordinatorEvent.createHandleAssignmentChangeEvent());
  }

  public void stop() {
    _connectors.forEach((connectorType, connector) -> connector.stop());

    while (_eventThread.isAlive()) {
      try {
        _eventThread.interrupt();
        _eventThread.join(EVENT_THREAD_JOIN_TIMEOUT);
      } catch (InterruptedException e) {
        LOG.warn("Exception caught while stopping coordinator", e);
      }
    }
    _adapter.disconnect();
  }

  public String getInstanceName() {
    return _adapter.getInstanceName();
  }

  /**
   * This method is called when the current datastream server instance becomes a leader.
   * There can only be only one leader in a datastream cluster.
   */
  @Override
  public void onBecomeLeader() {
    LOG.info("Coordinator::onBecomeLeader is called");
    // when an instance becomes a leader, make sure we don't miss new datastreams and
    // new assignment tasks that was not finished by the previous leader
    _eventQueue.put(CoordinatorEvent.createHandleNewDatastreamEvent());
    _eventQueue.put(CoordinatorEvent.createLeaderDoAssignmentEvent());
    LOG.info("Coordinator::onBecomeLeader completed successfully");
  }

  /**
   * This method is called when a new datastream server is added or existing datastream server goes down.
   */
  @Override
  public void onLiveInstancesChange() {
    LOG.info("Coordinator::onLiveInstancesChange is called");
    _eventQueue.put(CoordinatorEvent.createLeaderDoAssignmentEvent());
    LOG.info("Coordinator::onLiveInstancesChange completed successfully");
  }

  /**
   * This method is called when a new datastream is created. Right now we do not handle datastream updates/deletes.
   */
  @Override
  public void onDatastreamChange() {
    LOG.info("Coordinator::onDatastreamChange is called");
    // if there are new datastreams created, we need to trigger the topic creation logic
    _eventQueue.put(CoordinatorEvent.createHandleNewDatastreamEvent());
    _eventQueue.put(CoordinatorEvent.createLeaderDoAssignmentEvent());
    LOG.info("Coordinator::onDatastreamChange completed successfully");
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
    LOG.info("Coordinator::onAssignmentChange is called");
    _eventQueue.put(CoordinatorEvent.createHandleAssignmentChangeEvent());
    LOG.info("Coordinator::onAssignmentChange completed successfully");
  }

  private void handleAssignmentChange() {
    long startAt = System.currentTimeMillis();

    // when there is any change to the assignment for this instance. Need to find out what is the connector
    // type of the changed assignment, and then call the corresponding callback of the connector instance
    List<String> assignment = _adapter.getInstanceAssignment(_adapter.getInstanceName());

    LOG.info("START: Coordinator::handleAssignmentChange. Instance: " + _adapter.getInstanceName() + ", assignment: "
        + assignment);

    // all datastreamtask for all connector types
    Map<String, List<DatastreamTask>> currentAssignment = new HashMap<>();
    assignment.forEach(ds -> {
      DatastreamTask task = _adapter.getAssignedDatastreamTask(_adapter.getInstanceName(), ds);

      String connectorType = task.getConnectorType();
      if (!currentAssignment.containsKey(connectorType)) {
        currentAssignment.put(connectorType, new ArrayList<>());
      }
      currentAssignment.get(connectorType).add(task);
    });

    LOG.info(printAssignmentByType(currentAssignment));

    //
    // diff the currentAssignment with last saved assignment _allStreamsByConnectorType and make sure
    // the affected connectors are notified through the callback. There are following cases:
    // (1) a connector is removed of all assignment. This means the connector type does not exist in
    //     currentAssignment, but exist in the previous assignment in _allStreamsByConnectorType
    // (2) there are any changes of assignment for an existing connector type, including datastreamtasks
    //     added or removed. We do not handle the case when datastreamtask is updated. This include the
    //     case a connector previously doesn't have assignment but now has. This means the connector type
    //     is not contained in currentAssignment, but contained in _allStreamsByConnectorType
    //

    // case (1), find connectors that now doesn't handle any tasks
    List<String> oldConnectorList = new ArrayList<>();
    oldConnectorList.addAll(_allStreamsByConnectorType.keySet());
    List<String> newConnectorList = new ArrayList<>();
    newConnectorList.addAll(currentAssignment.keySet());

    List<String> deactivated = new ArrayList<>(oldConnectorList);
    deactivated.removeAll(newConnectorList);
    deactivated.forEach(connectorType -> _connectors.get(connectorType).onAssignmentChange(new ArrayList<>()));

    // case (2)
    newConnectorList
        .forEach(connectorType -> dispatchAssignmentChangeIfNeeded(connectorType, currentAssignment));

    // now save the current assignment
    _allStreamsByConnectorType = currentAssignment;

    long endAt = System.currentTimeMillis();

    LOG.info(String.format("END: Coordinator::handleAssignmentChange, Duration: %d milliseconds", endAt - startAt));
  }

  private void dispatchAssignmentChangeIfNeeded(String connectorType,
      Map<String, List<DatastreamTask>> currentAssignment) {

    ConnectorWrapper connector = _connectors.get(connectorType);
    List<DatastreamTask> assignment = currentAssignment.get(connectorType);

    // only call Connector onAssignment if it is needed
    boolean needed = false;

    if (!_allStreamsByConnectorType.containsKey(connectorType)) {
      // if there were no assignment in last cached version
      needed = true;
    } else if (_allStreamsByConnectorType.get(connectorType).size() != currentAssignment.get(connectorType).size()) {
      needed = true;
    } else {
      // if there are any difference in the list of assignment. Note that if there are no difference
      // between the two lists, then the connector onAssignmentChange() is not called.
      Set<DatastreamTask> oldSet = new HashSet<>(_allStreamsByConnectorType.get(connectorType));
      Set<DatastreamTask> newSet = new HashSet<>(currentAssignment.get(connectorType));
      Set<DatastreamTask> diffList1 = new HashSet<>(oldSet);
      Set<DatastreamTask> diffList2 = new HashSet<>(newSet);
      diffList1.removeAll(newSet);
      diffList2.removeAll(oldSet);
      if (diffList1.size() > 0 || diffList2.size() > 0) {
        needed = true;
      }
    }

    if (needed) {

      // Populate the event producers before calling the connector with the list of tasks.
      Map<DatastreamTask, EventProducer> producerMap = _eventProducerPool.getEventProducers(assignment, connectorType);
      for(DatastreamTask task : assignment) {
        DatastreamTaskImpl taskImpl = (DatastreamTaskImpl) task;
        if(producerMap.containsKey(task)) {
          taskImpl.setEventProducer(producerMap.get(task));
        } else {
          // TODO Set the status of the datastream task here.
          LOG.error("Event producer not created for datastream task " + task);
        }
      }

      connector.onAssignmentChange(assignment);
      if (connector.hasError()) {
        _eventQueue.put(CoordinatorEvent.createHandleInstanceErrorEvent(connector.getLastError()));
      }
    }
  }

  protected synchronized void handleEvent(CoordinatorEvent event) {
    LOG.info("START: Handle event " + event.getType() + ", Instance: " + _adapter.getInstanceName());

    try {
      switch (event.getType()) {
        case LEADER_DO_ASSIGNMENT:
          assignDatastreamTasksToInstances();
          break;

        case HANDLE_ASSIGNMENT_CHANGE:
          handleAssignmentChange();
          break;

        case HANDLE_NEW_DATASTREAM:
          handleNewDatastream();
          break;

        case HANDLE_INSTANCE_ERROR:
          handleInstanceError(event);

      }
    } catch (Throwable e) {
      LOG.error("ERROR: event + " + event + " failed.", e);
    }

    LOG.info("END: Handle event " + event);
  }

  // when we encouter an error, we need to persist the error message in zookeeper. We only persist the
  // first 10 messages. Why we put this logic in event loop instead of synchronously handle it? This
  // is because the same reason that can result in error can also result in the failure of persisting
  // the error message.
  private void handleInstanceError(CoordinatorEvent<String> event) {
    String msg = event.getEventData();
    _adapter.zkSaveInstanceError(msg);
  }

  // when there are new datastreams defined in DSM, we need to decide its target from the corresponding
  // connector, and write back the target to dsm tree in zookeeper. The assumption is that we only
  // detect the target of a datastream when it is first added. We do not handle the case at this point
  // when the datastream definition can be updated in DSM.
  private void handleNewDatastream() {
    // Allow further retry scheduling
    leaderDoAssignmentScheduled.set(false);

    // Get the list of all datastreams
    List<Datastream> newDatastreams = _adapter.getAllDatastreams();

    // do nothing if there is zero datastreams
    if (newDatastreams.size() == 0) {
      return;
    }

    try {
      // populateDatastreamDestination first dedups the datastreams with
      // the same source and only create destination for the unique ones.
      _destinationManager.populateDatastreamDestination(newDatastreams);

      // Update the znodes after destinations have been populated
      for (Datastream stream: newDatastreams) {
        if (stream.hasDestination() && !_adapter.updateDatastream(stream)) {
          LOG.error(String.format("Failed to update datastream destination for datastream %s, "
                  + "This datastream will not be scheduled for producing events ", stream.getName()));
        }
      }

      _eventQueue.put(CoordinatorEvent.createLeaderDoAssignmentEvent());
    } catch (Exception e) {
      LOG.error("Failed to update the destination of new datastreams.", e);

      // If there are any failure, we will need to schedule retry if
      // there is no pending retry scheduled already.
      if (leaderDoAssignmentScheduled.compareAndSet(false, true)) {
        LOG.warn("Schedule retry for handling new datastream");
        _executor.schedule(() -> _eventQueue.put(CoordinatorEvent.createHandleNewDatastreamEvent()),
                _config.getRetryIntervalMS(), TimeUnit.MILLISECONDS);
      }

    }
  }

  private void assignDatastreamTasksToInstances() {

    // get all current live instances
    List<String> liveInstances = _adapter.getLiveInstances();

    // get all streams that are assignable. Assignable datastreams are the ones
    // with a valid target. If there is no target, we cannot assign them to the connectors
    // because connectors do not have access to the producers and event collectors and
    // will assume any assigned tasks are ready to collect events.
    List<Datastream> allStreams =
        _adapter.getAllDatastreams().stream().filter(datastream -> datastream.getDestination() != null)
            .collect(Collectors.toList());

    // The inner map is used to dedup Datastreams with the same destination
    Map<String, Map<DatastreamDestination, Datastream>> streamsByConnectorType = new HashMap<>();

    for (Datastream ds : allStreams) {
      Map<DatastreamDestination, Datastream> streams =
              streamsByConnectorType.getOrDefault(ds.getConnectorType(), null);
      if (streams == null) {
        streams = new HashMap<>();
        streamsByConnectorType.put(ds.getConnectorType(), streams);
      }

      // Only keep the datastreams with unique destinations
      if (!streams.containsKey(ds.getDestination())) {
        streams.put(ds.getDestination(), ds);
      }
    }

    // for each connector type, call the corresponding assignment strategy
    Map<String, List<DatastreamTask>> assigmentsByInstance = new HashMap<>();
    streamsByConnectorType.forEach((connectorType, streams) -> {
      AssignmentStrategy strategy = _strategies.get(_connectors.get(connectorType));
      List<Datastream> streamList = new ArrayList<>(streams.values());
      Map<String, List<DatastreamTask>> tasksByConnectorAndInstance =
          strategy.assign(streamList, liveInstances, null);

      tasksByConnectorAndInstance.forEach((instance, assigned) -> {
        if (!assigmentsByInstance.containsKey(instance)) {
          assigmentsByInstance.put(instance, new ArrayList<>());
        }
        assigmentsByInstance.get(instance).addAll(assigned);
      });
    });

    // persist the assigned result to zookeeper. This means we will need to compare with the current
    // assignment and do remove and add znodes accordingly. In the case of zookeeper failure (when
    // it failed to create or delete znodes), we will do our best to continue the current process
    // and schedule a retry. The retry should be able to diff the remaining zookeeper work
    boolean succeeded = true;
    for (Map.Entry<String, List<DatastreamTask>> entry : assigmentsByInstance.entrySet()) {
      succeeded &= _adapter.updateInstanceAssignment(entry.getKey(), entry.getValue());
    }

    // schedule retry if failure
    if (!succeeded && !leaderDoAssignmentScheduled.get()) {
      LOG.info("Schedule retry for leader assigning tasks");
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
   * @param connector a connector that implements the Connector interface
   * @param strategy the assignment strategy deciding how to distribute datastream tasks among instances
   */
  public void addConnector(String connectorType, Connector connector, AssignmentStrategy strategy) {
    LOG.info("Add new connector of type " + connectorType + " to coordinator");

    if (_connectors.containsKey(connectorType)) {
      String err = "A connector of type " + connectorType + " already exists.";
      LOG.error(err);
      throw new IllegalArgumentException(err);
    }

    ConnectorWrapper connectorWrapper = new ConnectorWrapper(connectorType, connector);
    _connectors.put(connectorType, connectorWrapper);
    _strategies.put(connectorWrapper, strategy);
  }

  /**
   * Validate the datastream. Datastream management service will call this before writing the
   * Datastream into zookeeper. This method should ensure that the source has sufficient details.
   * @param datastream datastream for validation
   * @return result of the validation
   */
  public DatastreamValidationResult validateDatastream(Datastream datastream) {
    String connectorType = datastream.getConnectorType();
    ConnectorWrapper connector = _connectors.get(connectorType);
    if (connector == null) {
      return new DatastreamValidationResult("Invalid connector type: " + connectorType);
    }

    DatastreamValidationResult result = connector.validateDatastream(datastream);
    if (connector.hasError()) {
      _eventQueue.put(CoordinatorEvent.createHandleInstanceErrorEvent(connector.getLastError()));
    }

    return result;
  }

  private class CoordinatorEventProcessor extends Thread {
    @Override
    public void run() {
      LOG.info("START CoordinatorEventProcessor thread");
      while (!isInterrupted()) {
        try {
          CoordinatorEvent event = _eventQueue.take();
          if (event != null) {
            handleEvent(event);
          }
        } catch (InterruptedException e) {
          LOG.warn("CoordinatorEventProcess interrupted", e);
          interrupt();
        } catch (Throwable t) {
          LOG.error("CoordinatorEventProcessor failed", t);
        }
      }
      LOG.info("END CoordinatorEventProcessor");
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
