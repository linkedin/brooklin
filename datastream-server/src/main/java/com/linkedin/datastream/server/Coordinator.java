package com.linkedin.datastream.server;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.Set;
import java.util.HashSet;

import com.linkedin.datastream.common.Datastream;
import com.linkedin.datastream.common.DatastreamException;
import com.linkedin.datastream.common.VerifiableProperties;
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

  private final CoordinatorConfig _config;
  private final ZkAdapter _adapter;

  // mapping from connector instance to associated assignment strategy
  private final Map<Connector, AssignmentStrategy> _strategies = new HashMap<>();

  // mapping from connector type to connector instance
  private final Map<String, Connector> _connectors = new HashMap<>();

  private final DatastreamEventCollectorFactory _eventCollectorFactory;

  // all datastreams by connectory type. This is also valid for the coordinator leader
  // and it is stored after the leader finishes the datastream assignment
  private Map<String, List<DatastreamTask>> _allStreamsByConnectorType = new HashMap<>();

  public Coordinator(VerifiableProperties properties) throws DatastreamException {
    this(new CoordinatorConfig((properties)));
  }

  public Coordinator(CoordinatorConfig config) throws DatastreamException {
    _config = config;
    _adapter =
        new ZkAdapter(_config.getZkAddress(), _config.getCluster(), _config.getZkSessionTimeout(),
            _config.getZkConnectionTimeout(), this);
    _adapter.setListener(this);
    _eventCollectorFactory = new DatastreamEventCollectorFactory(config.getConfigProperties());
  }

  public void start() {
    _adapter.connect();
    _strategies.forEach((connector, strategy) -> connector.start(_eventCollectorFactory));
  }

  public void stop() {
    _strategies.forEach((connector, strategy) -> connector.stop());
    _adapter.disconnect();
  }

  public String getInstanceName() {
    return _adapter.getInstanceName();
  }

  @Override
  public void onBecomeLeader() {
    assignDatastreamTasksToInstances();
  }

  @Override
  public void onLiveInstancesChange() {
    assignDatastreamTasksToInstances();
  }

  @Override
  public void onDatastreamChange() {
    assignDatastreamTasksToInstances();
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
    // when there is any change to the assignment for this instance. Need to find out what is the connector
    // type of the changed assignment, and then call the corresponding callback of the connector instance
    List<String> assignment = _adapter.getInstanceAssignment(_adapter.getInstanceName());

    // all datastreamtask for all connector types
    Map<String, List<DatastreamTask>> currentAssignment = new HashMap<>();
    assignment.forEach(ds -> {
      DatastreamTask task = _adapter.getAssignedDatastreamTask(_adapter.getInstanceName(), ds);

      if (!currentAssignment.containsKey(task.getConnectorType())) {
        currentAssignment.put(task.getConnectorType(), new ArrayList<DatastreamTask>());
      }
      currentAssignment.get(task.getConnectorType()).add(task);

    });

    DatastreamContext context = new DatastreamContextImpl(_adapter);

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
    deactivated.forEach(connectorType -> {
      _connectors.get(connectorType).onAssignmentChange(context, new ArrayList<>());
    });

    // case (2)
    newConnectorList.forEach(connectorType -> {
      dispatchAssignmentChangeIfNeeded(connectorType, context, currentAssignment);
    });

    // now save the current assignment
    _allStreamsByConnectorType = currentAssignment;
  }

  private void dispatchAssignmentChangeIfNeeded(String connectorType, DatastreamContext context,
      Map<String, List<DatastreamTask>> currentAssignment) {
    if (!_allStreamsByConnectorType.containsKey(connectorType)) {
      // if there were no assignment in last cached version
      _connectors.get(connectorType).onAssignmentChange(context, currentAssignment.get(connectorType));
    } else if (_allStreamsByConnectorType.get(connectorType).size() != currentAssignment.get(connectorType).size()) {
      // if the number of assignment is different
      _connectors.get(connectorType).onAssignmentChange(context, currentAssignment.get(connectorType));
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
        _connectors.get(connectorType).onAssignmentChange(context, currentAssignment.get(connectorType));
      }
    }
  }

  private void assignDatastreamTasksToInstances() {

    // get all current live instances
    List<String> liveInstances = _adapter.getLiveInstances();

    // get all data streams that is assignable
    List<Datastream> allStreams = _adapter.getAllDatastreams();

    Map<String, List<Datastream>> streamsByConnectoryType = new HashMap<>();

    for (Datastream ds : allStreams) {
      if (!streamsByConnectoryType.containsKey(ds.getConnectorType())) {
        streamsByConnectoryType.put(ds.getConnectorType(), new ArrayList<>());
      }

      streamsByConnectoryType.get(ds.getConnectorType()).add(ds);
    }

    // for each connector type, call the corresponding assignment strategy
    Map<String, List<DatastreamTask>> assigmentsByInstance = new HashMap<>();
    streamsByConnectoryType.forEach((connectorType, streams) -> {
      AssignmentStrategy strategy = _strategies.get(_connectors.get(connectorType));
      Map<String, List<DatastreamTask>> tasksByConnectorAndInstance =
          strategy.assign(streamsByConnectoryType.get(connectorType), liveInstances, null);

      tasksByConnectorAndInstance.forEach((instance, assigned) -> {
        if (!assigmentsByInstance.containsKey(instance)) {
          assigmentsByInstance.put(instance, new ArrayList<>());
        }
        assigmentsByInstance.get(instance).addAll(assigned);
      });
    });

    // persist the assigned result to zookeeper. This means we will need to compare with the current
    // assignment and do remove and add znodes accordingly.
    assigmentsByInstance.forEach((instance, assigned) -> {
      _adapter.updateInstanceAssignment(instance, assigned);
    });
  }

  /**
   * Add a connector to the coordinator. A coordinator can handle multiple type of connectors, but only one
   * connector per connector type.
   *
   * @param connector a connector that implements the Connector interface
   * @param strategy the assignment strategy deciding how to distribute datastream tasks among instances
   */
  public void addConnector(Connector connector, AssignmentStrategy strategy) {
    String connectorType = connector.getConnectorType();
    LOG.info("Add new connector of type " + connectorType + " to coordinator");

    if (_connectors.containsKey(connectorType)) {
      String err = "A connector of type " + connectorType + " already exists.";
      LOG.error(err);
      throw new IllegalArgumentException(err);
    }

    _connectors.put(connectorType, connector);
    _strategies.put(connector, strategy);
  }

  /**
   * Validate the datastream. Datastream management service will call this before writing the
   * Datastream into zookeeper. This method should ensure that the source has sufficient details.
   * @param datastream datastream for validation
   * @return result of the validation
   */
  public DatastreamValidationResult validateDatastream(Datastream datastream) {
    String connectorType = datastream.getConnectorType();
    Connector connector = _connectors.get(connectorType);
    if (connector == null) {
      return new DatastreamValidationResult("Invalid connector type: " + connectorType);
    }
    return connector.validateDatastream(datastream);
  }
}
