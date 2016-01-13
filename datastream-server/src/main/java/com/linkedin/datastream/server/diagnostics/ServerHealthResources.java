package com.linkedin.datastream.server.diagnostics;

import com.linkedin.datastream.diagnostics.ConnectorHealth;
import com.linkedin.datastream.diagnostics.ConnectorHealthArray;
import com.linkedin.datastream.diagnostics.ServerHealth;
import com.linkedin.datastream.diagnostics.TaskHealth;
import com.linkedin.datastream.diagnostics.TaskHealthArray;
import com.linkedin.datastream.server.Coordinator;
import com.linkedin.datastream.server.DatastreamServer;
import com.linkedin.datastream.server.DatastreamTask;
import com.linkedin.restli.server.annotations.RestLiSimpleResource;
import com.linkedin.restli.server.resources.SimpleResourceTemplate;

@RestLiSimpleResource(name = "health", namespace = "com.linkedin.datastream.server.diagnostics")
public class ServerHealthResources extends SimpleResourceTemplate<ServerHealth> {

  private final DatastreamServer _server;
  private final Coordinator _coordinator;

  public ServerHealthResources(DatastreamServer datastreamServer) {
    _server = datastreamServer;
    _coordinator = datastreamServer.getCoordinator();
  }

  @Override
  public ServerHealth get() {
    return buildServerHealth();
  }

  private ServerHealth buildServerHealth() {
    ServerHealth health = new ServerHealth();
    health.setInstanceName(_server.getCoordinator().getInstanceName());
    health.setClusterName(_server.getCoordinator().getClusterName());
    health.setConnectors(buildConnectorHealth());
    return health;
  }

  private ConnectorHealthArray buildConnectorHealth() {
    ConnectorHealthArray allConnectorsHealth = new ConnectorHealthArray();

    for(String connectorType : _coordinator.getTasksByConnectorType().keySet()) {
      ConnectorHealth connectorHealth = new ConnectorHealth();
      connectorHealth.setConnectorType(connectorType);
      connectorHealth.setTasks(buildTasksHealthForConnectorType(connectorType));
      allConnectorsHealth.add(connectorHealth);
    }

    return allConnectorsHealth;
  }

  private TaskHealthArray buildTasksHealthForConnectorType(String connectorType) {
    TaskHealthArray allTasksHealth = new TaskHealthArray();

    for(DatastreamTask task : _coordinator.getTasksByConnectorType().get(connectorType)) {
      TaskHealth taskHealth = new TaskHealth();
      taskHealth.setDatastreams(String.join(",", task.getDatastreams()));
      taskHealth.setDestination(task.getDatastreamDestination().getConnectionString());
      taskHealth.setSource(task.getDatastreamSource().getConnectionString());
      taskHealth.setStatusCode(task.getStatus().getCode().toString());
      taskHealth.setStatusMessage(task.getStatus().getMessage());
      taskHealth.setName(task.getDatastreamTaskName());
      taskHealth.setPartitions(task.getPartitions().toString());
      taskHealth.setSourceCheckpoint(task.getCheckpoints().toString());
      allTasksHealth.add(taskHealth);
    }

    return allTasksHealth;
  }
}
