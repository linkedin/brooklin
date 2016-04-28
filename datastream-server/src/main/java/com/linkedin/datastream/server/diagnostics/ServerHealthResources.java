package com.linkedin.datastream.server.diagnostics;

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

  private static final Logger LOG = LoggerFactory.getLogger(ServerHealthResources.class);

  private final DatastreamServer _server;
  private final Coordinator _coordinator;

  public ServerHealthResources(DatastreamServer datastreamServer) {
    _server = datastreamServer;
    _coordinator = datastreamServer.getCoordinator();
  }

  @Override
  public ServerHealth get() {
    LOG.info("Get request for serverHealth");
    ServerHealth health = buildServerHealth();
    LOG.info("Server Health: " + health.toString());
    return health;
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
    Collection<DatastreamTask> tasks = _coordinator.getDatastreamTasks();
    List<String> connectors = tasks
        .stream()
        .map(DatastreamTask::getConnectorType)
        .distinct()
        .collect(Collectors.toList());

    for (String connectorType : connectors) {
      ConnectorHealth connectorHealth = new ConnectorHealth();
      connectorHealth.setConnectorType(connectorType);
      connectorHealth.setTasks(buildTasksHealthForConnectorType(connectorType));
      allConnectorsHealth.add(connectorHealth);
    }

    return allConnectorsHealth;
  }

  private TaskHealthArray buildTasksHealthForConnectorType(String connectorType) {
    TaskHealthArray allTasksHealth = new TaskHealthArray();

    _coordinator.getDatastreamTasks().stream()
        .filter(t -> t.getConnectorType().equals(connectorType)).forEach(task -> {
      TaskHealth taskHealth = new TaskHealth();
      taskHealth.setDatastreams(task.getDatastreams().toString());
      taskHealth.setDatastreams(task.getDatastreams().stream()
          .collect(Collectors.joining(",")));

      if (task.getDatastreamDestination() != null) {
        taskHealth.setDestination(task.getDatastreamDestination().getConnectionString());
      }

      if (task.getDatastreamSource() != null) {
        taskHealth.setSource(task.getDatastreamSource().getConnectionString());
      }

      if (task.getStatus() != null) {
        taskHealth.setStatusCode(task.getStatus().getCode().toString());
        taskHealth.setStatusMessage(task.getStatus().getMessage());
      }

      taskHealth.setName(task.getDatastreamTaskName());
      taskHealth.setPartitions(task.getPartitions().toString());
      taskHealth.setSourceCheckpoint(task.getCheckpoints().toString());
      allTasksHealth.add(taskHealth);
    });

    return allTasksHealth;
  }
}
