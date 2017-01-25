package com.linkedin.brooklin.eventhub;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import com.linkedin.datastream.common.Datastream;
import com.linkedin.datastream.common.DatastreamDestination;
import com.linkedin.datastream.server.DatastreamTask;
import com.linkedin.datastream.server.api.connector.DatastreamValidationException;
import com.linkedin.datastream.server.api.transport.TransportProvider;
import com.linkedin.datastream.server.api.transport.TransportProviderAdmin;


public class EventHubTransportProviderAdmin implements TransportProviderAdmin {

  private Map<DatastreamTask, EventHubTransportProvider> _transportProviders = new HashMap<>();

  public EventHubTransportProviderAdmin(String transportProviderName, Properties transportProviderProperties) {
  }

  @Override
  public synchronized TransportProvider assignTransportProvider(DatastreamTask task) {
    EventHubDestination destination = new EventHubDestination(task.getDatastreams().get(0));
    if (_transportProviders.containsKey(task)) {
      return _transportProviders.get(task);
    }

    EventHubTransportProvider tp = new EventHubTransportProvider(destination, task.getPartitions());
    _transportProviders.put(task, tp);
    return tp;
  }

  @Override
  public synchronized void unassignTransportProvider(DatastreamTask task) {
    EventHubTransportProvider tp = _transportProviders.get(task);
    if (tp != null) {
      tp.close();
    }
    _transportProviders.remove(task);
  }

  @Override
  public void initializeDestinationForDatastream(Datastream datastream) throws DatastreamValidationException {

    if (!datastream.hasDestination()) {
      datastream.setDestination(new DatastreamDestination());
    }

    DatastreamDestination destination = datastream.getDestination();

    if (!destination.hasConnectionString() || destination.getConnectionString().isEmpty()) {
      throw new DatastreamValidationException("Datastream Destination connection string is not populated.");
    }

    if (!destination.hasPartitions() || destination.getPartitions() <= 0) {
      throw new DatastreamValidationException("Datastream destination partitions is not populated");
    }

    if (!datastream.hasMetadata() || !datastream.getMetadata().containsKey(EventHubDestination.SHARED_ACCESS_KEY_NAME)
        || !datastream.getMetadata().containsKey(EventHubDestination.SHARED_ACCESS_KEY)) {
      throw new DatastreamValidationException(
          "Datastream doesn't have the access keys configured in metadata to access the event hub destination");
    }
  }

  @Override
  public void createDestination(Datastream datastream) {
    throw new UnsupportedOperationException("Create destination is not supported for event hub.");
  }

  @Override
  public void dropDestination(Datastream datastream) {
    throw new UnsupportedOperationException("Drop destination is not supported for event hub.");
  }

  @Override
  public Duration getRetention(Datastream datastream) {
    throw new UnsupportedOperationException("getRetention is not supported for event hub.");
  }
}
