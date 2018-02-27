package com.linkedin.datastream.connectors.kafka.mirrormaker;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;
import java.util.Set;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.PartitionInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.datastream.common.Datastream;
import com.linkedin.datastream.common.DatastreamMetadataConstants;
import com.linkedin.datastream.common.DatastreamSource;
import com.linkedin.datastream.common.DatastreamUtils;
import com.linkedin.datastream.common.JsonUtils;
import com.linkedin.datastream.connectors.kafka.AbstractKafkaBasedConnectorTask;
import com.linkedin.datastream.connectors.kafka.AbstractKafkaConnector;
import com.linkedin.datastream.connectors.kafka.KafkaConnectionString;
import com.linkedin.datastream.connectors.kafka.KafkaConnectorTask;
import com.linkedin.datastream.metrics.BrooklinMetricInfo;
import com.linkedin.datastream.server.DatastreamTask;
import com.linkedin.datastream.server.api.connector.DatastreamValidationException;


/**
 * KafkaMirrorMakerConnector is similar to KafkaConnector but it has the ability to consume from multiple topics in a
 * cluster via regular expression pattern source, and it has the ability to produce to multiple topics in the
 * destination cluster.
 */
public class KafkaMirrorMakerConnector extends AbstractKafkaConnector {
  private static final Logger LOG = LoggerFactory.getLogger(KafkaMirrorMakerConnector.class);

  protected static final String IS_FLUSHLESS_MODE_ENABLED = "isFlushlessModeEnabled";
  private final boolean _isFlushlessModeEnabled;

  public KafkaMirrorMakerConnector(String connectorName, Properties config) {
    super(connectorName, config, LOG);
    _isFlushlessModeEnabled =
        Boolean.parseBoolean(config.getProperty(IS_FLUSHLESS_MODE_ENABLED, Boolean.FALSE.toString()));
  }

  @Override
  protected AbstractKafkaBasedConnectorTask createKafkaBasedConnectorTask(DatastreamTask task) {
    return _isFlushlessModeEnabled ? new FlushlessKafkaMirrorMakerConnectorTask(_consumerFactory, _consumerProps, task,
        _commitIntervalMillis, RETRY_SLEEP_DURATION, _retryCount)
        : new KafkaMirrorMakerConnectorTask(_consumerFactory, _consumerProps, task, _commitIntervalMillis,
            RETRY_SLEEP_DURATION, _retryCount);
  }

  @Override
  public void initializeDatastream(Datastream stream, List<Datastream> allDatastreams)
      throws DatastreamValidationException {
    // verify that the MirrorMaker Datastream will not be re-used
    if (DatastreamUtils.isReuseAllowed(stream)) {
      throw new DatastreamValidationException(
          String.format("Destination reuse is not allowed for connector %s. Datastream: %s", stream.getConnectorName(),
              stream));
    }

    // verify that BYOT is not used
    if (DatastreamUtils.isUserManagedDestination(stream)) {
      throw new DatastreamValidationException(
          String.format("BYOT is not allowed for connector %s. Datastream: %s", stream.getConnectorName(), stream));
    }

    if (!DatastreamUtils.isConnectorManagedDestination(stream)) {
      stream.getMetadata()
          .put(DatastreamMetadataConstants.IS_CONNECTOR_MANAGED_DESTINATION_KEY, Boolean.TRUE.toString());
    }

    if (DatastreamUtils.isReuseAllowed(stream)) {
      throw new DatastreamValidationException(
          String.format("Destination reuse is not allowed for connector %s. Datastream: %s", stream.getConnectorName(),
              stream));
    }

    // verify that the source regular expression can be compiled
    KafkaConnectionString connectionString = KafkaConnectionString.valueOf(stream.getSource().getConnectionString());
    try {
      Pattern.compile(connectionString.getTopicName());
    } catch (PatternSyntaxException e) {
      throw new DatastreamValidationException(
          String.format("Regular expression in Datastream source connection string (%s) is ill-formatted.",
              stream.getSource().getConnectionString()), e);
    }
  }

  @Override
  public String getDestinationName(Datastream stream) {
    // return %s so that topic can be inserted into the destination string at produce time
    return "%s";
  }

  @Override
  public List<BrooklinMetricInfo> getMetricInfos() {
    return Collections.unmodifiableList(KafkaMirrorMakerConnectorTask.getMetricInfos());
  }

  @Override
  public void validateUpdateDatastreams(List<Datastream> datastreams, List<Datastream> allDatastreams)
      throws DatastreamValidationException {
    super.validateUpdateDatastreams(datastreams, allDatastreams);

    // validate for paused partitions
    validatePausedPartitions(datastreams, allDatastreams);
  }

  private void validatePausedPartitions(List<Datastream> datastreams, List<Datastream> allDatastreams)
      throws DatastreamValidationException {
    for (Datastream ds : datastreams) {
      validatePausedPartitions(ds, allDatastreams);
    }
  }

  private void validatePausedPartitions(Datastream datastream, List<Datastream> allDatastreams)
      throws DatastreamValidationException {
    Map<String, Set<String>> pausedSourcePartitionsMap = DatastreamUtils.getDatastreamSourcePartitions(datastream);

    for (String source : pausedSourcePartitionsMap.keySet()) {
      Set<String> newPartitions = pausedSourcePartitionsMap.get(source);

      // Validate that partitions actually exist and convert any "*" to actual list of partitions.
      // For that, get the list of existing partitions first.
      List<PartitionInfo> partitionInfos = getKafkaTopicPartitions(datastream, source);
      Set<String> allPartitions = new HashSet<>();
      for (PartitionInfo info : partitionInfos) {
        allPartitions.add(String.valueOf(info.partition()));
      }

      // if there is any * in the new list, just convert it to actual list of partitions.
      if (newPartitions.contains(DatastreamMetadataConstants.REGEX_PAUSE_ALL_PARTITIONS_IN_A_TOPIC)) {
        newPartitions.clear();
        newPartitions.addAll(allPartitions);
      } else {
        // Else make sure there aren't any partitions that don't exist.
        newPartitions.retainAll(allPartitions);
      }
    }

    // Now write back the set to datastream
    datastream.getMetadata()
        .put(DatastreamMetadataConstants.PAUSED_SOURCE_PARTITIONS_KEY, JsonUtils.toJson(pausedSourcePartitionsMap));
  }

  private List<PartitionInfo> getKafkaTopicPartitions(Datastream datastream, String topic)
      throws DatastreamValidationException {
    List<PartitionInfo> partitionInfos = null;

    DatastreamSource source = datastream.getSource();
    String connectionString = source.getConnectionString();

    KafkaConnectionString parsed = KafkaConnectionString.valueOf(connectionString);

    try (Consumer<?, ?> consumer = KafkaConnectorTask.createConsumer(_consumerFactory, _consumerProps,
        "MmPartitionFinder", parsed)) {
      partitionInfos = consumer.partitionsFor(topic);
      if (partitionInfos == null) {
        throw new DatastreamValidationException("Can't get partition info from kafka for topic: " + topic);
      }
    } catch (Exception e) {
      throw new DatastreamValidationException(
          "Exception received while retrieving info on kafka topic partitions: " + e);
    }

    return partitionInfos;
  }
}
