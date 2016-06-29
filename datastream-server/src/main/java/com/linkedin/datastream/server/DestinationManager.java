package com.linkedin.datastream.server;

import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.commons.lang.Validate;

import com.linkedin.datastream.common.Datastream;
import com.linkedin.datastream.common.DatastreamDestination;
import com.linkedin.datastream.common.DatastreamMetadataConstants;
import com.linkedin.datastream.common.DatastreamSource;
import com.linkedin.datastream.common.VerifiableProperties;
import com.linkedin.datastream.server.api.transport.TransportException;
import com.linkedin.datastream.server.api.transport.TransportProvider;


/**
 * Class that uses the Transport provider to manage the topics used by the datastream
 */
public class DestinationManager {
  private static final Logger LOG = LoggerFactory.getLogger(DestinationManager.class.getName());
  private static final int DEFAULT_NUMBER_PARTITIONS = 1;
  private static final String REGEX_NON_ALPHA = "[^\\w]";
  private static final String DESTINATION_DOMAIN = "destination";

  private final TransportProvider _transportProvider;
  private final boolean _reuseExistingTopic;

  public DestinationManager(boolean reuseExistingTopic, TransportProvider transportProvider) {
    _reuseExistingTopic = reuseExistingTopic;
    _transportProvider = transportProvider;
  }

  /**
   * populates the datastream destination for the newly created datastreams.
   * Caller (Datastream leader) should pass in all the datastreams present in the system.
   * This method will take care of de-duping the datastreams, i.e. if there is an existing
   * datastream with the same source, they will use the same destination.
   * @param datastreams All datastreams in the current system.
   */
  public void populateDatastreamDestination(List<Datastream> datastreams)
      throws TransportException {
    Validate.notNull(datastreams, "Datastream should not be null");

    HashMap<DatastreamSource, Datastream> sourceStreamMapping = new HashMap<>();
    datastreams.stream().filter(d -> d.hasDestination() && d.getDestination().hasConnectionString() &&
        !d.getDestination().getConnectionString().isEmpty()).forEach(d -> sourceStreamMapping.put(d.getSource(), d));

    LOG.debug("Datastream Source -> Datastream mapping before populating new datastream destinations",
        sourceStreamMapping);

    for (Datastream datastream : datastreams) {
      if (datastream.hasDestination() && datastream.getDestination().hasConnectionString() &&
          !datastream.getDestination().getConnectionString().isEmpty()) {
        continue;
      }

      boolean topicReuse = _reuseExistingTopic;
      if (datastream.hasMetadata()) {
        topicReuse = Boolean.parseBoolean(datastream.getMetadata()
            .getOrDefault(DatastreamMetadataConstants.REUSE_EXISTING_DESTINATION_KEY,
                String.valueOf(_reuseExistingTopic)));
      }

      // De-dup the datastreams, Set the destination for the duplicate datastreams same as the existing ones.
      Datastream existingStream = sourceStreamMapping.getOrDefault(datastream.getSource(), null);
      if (topicReuse && existingStream != null &&
          existingStream.getConnectorName().equals(datastream.getConnectorName())) {
        DatastreamDestination destination = existingStream.getDestination();
        LOG.info(String.format("Datastream %s has same source as existing datastream, Setting the destination %s",
            datastream.getName(), destination));
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
      } else {
        String connectionString = createTopic(datastream);
        LOG.info(String.format(
            "Datastream %s has an unique source or topicReuse (%s) is set to true, Creating a new destination topic %s",
            datastream.getName(), topicReuse, connectionString));
        sourceStreamMapping.put(datastream.getSource(), datastream);
      }
    }

    LOG.debug("Datastream Source -> Destination mapping after the populating new datastream destinations",
        sourceStreamMapping);
  }

  private String createTopic(Datastream datastream)
      throws TransportException {
    Properties datastreamProperties = new Properties();
    if (datastream.hasMetadata()) {
      datastreamProperties.putAll(datastream.getMetadata());
    }
    Properties topicProperties = new VerifiableProperties(datastreamProperties).getDomainProperties(DESTINATION_DOMAIN);
    int numberOfPartitions = DEFAULT_NUMBER_PARTITIONS;

    // if the number of partitions is already set on the destination then use that.
    if (datastream.hasDestination() && datastream.getDestination().hasPartitions()) {
      numberOfPartitions = datastream.getDestination().getPartitions();
    } else if (datastream.hasSource() && datastream.getSource().hasPartitions()) {
      // If the number of partitions is not set in destination but set in source, use that.
      numberOfPartitions = datastream.getSource().getPartitions();
    }

    String connectionString =
        _transportProvider.createTopic(getTopicName(datastream), numberOfPartitions, topicProperties);

    DatastreamDestination destination = new DatastreamDestination();
    destination.setConnectionString(connectionString);
    destination.setPartitions(numberOfPartitions);
    datastream.setDestination(destination);

    // Set destination creation time and retention
    datastream.getMetadata()
        .put(DatastreamMetadataConstants.DESTINATION_CREATION_MS, String.valueOf(Instant.now().toEpochMilli()));

    Duration retention = _transportProvider.getRetention(connectionString);
    if (retention != null) {
      datastream.getMetadata()
          .put(DatastreamMetadataConstants.DESTINATION_RETENION_MS, String.valueOf(retention.toMillis()));
    }

    return connectionString;
  }

  /**
   * Example 1:
   *  [source] connector://cluster/db/table/partition
   *  [destination] connector_cluster_db_table_partition
   *
   * Example 2:
   *  [source] connector://cluster/db/table/*
   *  [destination] connector_cluster_db_table_
   */
  private String getTopicName(Datastream datastream) {
    URI sourceUri = URI.create(datastream.getSource().getConnectionString());
    String path;
    if (sourceUri.getAuthority() != null) {
      path = sourceUri.getAuthority() + sourceUri.getPath();
    } else {
      path = sourceUri.getPath().substring(1); // strip leading slash
    }
    // Replace / with _ and strip out all non-alphanumeric chars
    path = path.replace("/", "_").replaceAll(REGEX_NON_ALPHA, "");
    // Include the connector type and random UUID
    return String.join("_", datastream.getConnectorName(), path, UUID.randomUUID().toString());
  }

  /**
   * Delete the datastream destination for a particular datastream.
   * Caller should pass in all the datastreams present in the system.
   * This method will ensure that there are no other references to the destination before deleting it.
   * @param datastream Datastream whose destination needs to be deleted.
   * @param allDatastreams All the datastreams in the system.
   */
  public void deleteDatastreamDestination(Datastream datastream, List<Datastream> allDatastreams)
      throws TransportException {
    Validate.notNull(datastream, "Datastream should not be null");
    Validate.notNull(datastream.getDestination(), "Datastream destination should not be null");
    Validate.notNull(allDatastreams, "allDatastreams should not be null");
    Stream<Datastream> duplicateDatastreams = allDatastreams.stream()
        .filter(d -> d.getDestination().equals(datastream.getDestination()) && !d.getName()
            .equalsIgnoreCase(datastream.getName()));

    // If there are no datastreams using the same destination, then delete the topic.
    if (duplicateDatastreams.count() == 0) {
      _transportProvider.dropTopic(datastream.getDestination().getConnectionString());
    } else {
      LOG.info(String.format("There are existing datastreams %s with the same destination (%s) as datastream %s ",
          duplicateDatastreams, datastream.getDestination(), datastream.getName()));
    }
  }
}
