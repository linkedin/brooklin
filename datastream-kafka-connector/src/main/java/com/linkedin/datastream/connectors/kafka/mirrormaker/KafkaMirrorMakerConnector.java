package com.linkedin.datastream.connectors.kafka.mirrormaker;

import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.datastream.common.Datastream;
import com.linkedin.datastream.common.DatastreamMetadataConstants;
import com.linkedin.datastream.common.DatastreamUtils;
import com.linkedin.datastream.connectors.kafka.AbstractKafkaBasedConnectorTask;
import com.linkedin.datastream.connectors.kafka.AbstractKafkaConnector;
import com.linkedin.datastream.connectors.kafka.KafkaConnectionString;
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
          String.format("BYOT is not allowed for connector %s. Datastream: %s", stream.getConnectorName(),
              stream));
    }

    if (!DatastreamUtils.isConnectorManagedDestination(stream)) {
      stream.getMetadata()
          .put(DatastreamMetadataConstants.IS_CONNECTOR_MANAGED_DESTINATION_KEY, Boolean.TRUE.toString());
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


}
