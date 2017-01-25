package com.linkedin.brooklin.samza;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.datastream.common.Datastream;
import com.linkedin.datastream.common.DatastreamDestination;
import com.linkedin.datastream.server.DatastreamTask;
import com.linkedin.datastream.server.api.connector.DatastreamValidationException;
import com.linkedin.datastream.server.api.transport.TransportProvider;
import com.linkedin.datastream.server.api.transport.TransportProviderAdmin;


/**
 * Samza transport provider admin
 */
public class SamzaTransportProviderAdmin implements TransportProviderAdmin {
  private static final Logger LOG = LoggerFactory.getLogger(SamzaTransportProviderAdmin.class);

  private final Properties _transportProviderProperties;
  private final String _tpName;

  private Map<DatastreamTask, SamzaTransportProvider> _transportProviders = new HashMap<>();

  public SamzaTransportProviderAdmin(String tpName, Properties transportProviderProperties) {
    _tpName = tpName;
    _transportProviderProperties = transportProviderProperties;
  }

  @Override
  public TransportProvider assignTransportProvider(DatastreamTask task) {
    if (!_transportProviders.containsKey(task)) {
      Datastream ds = task.getDatastreams().get(0);
      _transportProviders.put(task, new SamzaTransportProvider(_transportProviderProperties, ds));
    }

    return _transportProviders.get(task);
  }

  @Override
  public void unassignTransportProvider(DatastreamTask task) {
    if (_transportProviders.containsKey(task)) {
      SamzaTransportProvider tp = _transportProviders.get(task);
      tp.close();
      _transportProviders.remove(task);
    }
  }

  @Override
  public void initializeDestinationForDatastream(Datastream datastream) throws DatastreamValidationException {
    if (!datastream.hasDestination()) {
      datastream.setDestination(new DatastreamDestination());
    }

    DatastreamDestination destination = datastream.getDestination();

    if (!destination.hasConnectionString() || destination.getConnectionString().isEmpty()) {
      throw new DatastreamValidationException("Datastream destination connection string is not populated.");
    }

    if (!destination.hasPartitions() || destination.getPartitions() <= 0) {
      throw new DatastreamValidationException("Datastream destination partitions is not populated.");
    }

    if (!datastream.getTransportProviderName().equals(_tpName)) {
      throw new DatastreamValidationException("Datastream destination transport provider name is not valid.");
    }

    // Validate the datastream by instantiating the provider and closing it.
    TransportProvider provider = new SamzaTransportProvider(_transportProviderProperties, datastream);
    provider.close();
  }

  @Override
  public void createDestination(Datastream datastream) {
    // Samza doesn't provide APIs for creating the destination.
  }

  @Override
  public void dropDestination(Datastream datastream) {
    // Samza doesn't provide APIs for dropping the destination.
  }

  @Override
  public Duration getRetention(Datastream datastream) {
    throw new UnsupportedOperationException("GetRetention is not supported with SamzaTransportProvider");
  }
}
