package com.linkedin.datastream.server;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.lang.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.datastream.common.Datastream;
import com.linkedin.datastream.common.DatastreamDestination;
import com.linkedin.datastream.common.DatastreamMetadataConstants;
import com.linkedin.datastream.common.DatastreamSource;
import com.linkedin.datastream.server.api.connector.DatastreamValidationException;
import com.linkedin.datastream.server.api.transport.TransportException;
import com.linkedin.datastream.server.api.transport.TransportProviderAdmin;


/**
 * Class that uses the Transport provider to manage the topics used by the datastream
 */
public class DestinationManager {
  private static final Logger LOG = LoggerFactory.getLogger(DestinationManager.class.getName());

  private final Map<String, TransportProviderAdmin> _transportProviderAdmins;
  private final boolean _reuseExistingTopic;

  public DestinationManager(boolean reuseExistingTopic, Map<String, TransportProviderAdmin> transportProviderAdmins) {
    _reuseExistingTopic = reuseExistingTopic;
    _transportProviderAdmins = transportProviderAdmins;
  }

  /**
   * populates the datastream destination for the newly created datastreams.
   * Caller (Datastream leader) should pass in all the datastreams present in the system.
   * This method will take care of de-duping the datastreams, i.e. if there is an existing
   * datastream with the same source, they will use the same destination.
   * @param datastream the datastream whose destination to be populated.
   * @param datastreams All datastreams in the current system.
   */
  public void populateDatastreamDestination(Datastream datastream, List<Datastream> datastreams)
      throws DatastreamValidationException {
    Validate.notNull(datastream, "Datastream should not be null");
    Validate.notNull(datastreams, "Datastreams should not be null");

    if (datastream.hasDestination() && datastream.getDestination().hasConnectionString() &&
        !datastream.getDestination().getConnectionString().isEmpty()) {
      return;
    }

    HashMap<DatastreamSource, Datastream> sourceStreamMapping = new HashMap<>();
    datastreams.stream().filter(d -> d.hasDestination() && d.getDestination().hasConnectionString() &&
        !d.getDestination().getConnectionString().isEmpty()).forEach(d -> sourceStreamMapping.put(d.getSource(), d));

    LOG.debug("Datastream Source -> Datastream mapping before populating new datastream destinations: %s",
        sourceStreamMapping);

    boolean topicReuse = _reuseExistingTopic;
    if (datastream.hasMetadata()) {
      topicReuse = Boolean.parseBoolean(datastream.getMetadata()
          .getOrDefault(DatastreamMetadataConstants.REUSE_EXISTING_DESTINATION_KEY,
              String.valueOf(_reuseExistingTopic)));
    }

    // De-dup the datastreams, Set the destination for the duplicate datastreams same as the existing ones.
    Datastream existingStream = sourceStreamMapping.getOrDefault(datastream.getSource(), null);
    if (topicReuse && existingStream != null &&
        existingStream.getConnectorName().equals(datastream.getConnectorName())
        && existingStream.getTransportProviderName().equals(datastream.getTransportProviderName())) {
      LOG.info(String.format("Datastream: %s has same source as existing datastream: %s, Setting the destination %s",
          datastream.getName(), existingStream.getName(), existingStream.getDestination()));
      populateDatastreamDestinationFromExistingDatastream(datastream, existingStream);
    } else {
      _transportProviderAdmins.get(datastream.getTransportProviderName())
          .initializeDestinationForDatastream(datastream);

      LOG.info(
          String.format("Datastream %s has an unique source or topicReuse (%s) is set to true, Creating a new topic %s",
              datastream.getName(), topicReuse, datastream.getDestination()));

      sourceStreamMapping.put(datastream.getSource(), datastream);
    }

    LOG.debug("Datastream Source -> Destination mapping after the populating new datastream destinations: %s",
        sourceStreamMapping);
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

  public String createTopic(Datastream datastream) throws TransportException {
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
      LOG.warn("Transport doesn't support mechanism to get retention, Unable to populate retention in datastream", e);
    }

    return datastream.getDestination().getConnectionString();
  }
}
