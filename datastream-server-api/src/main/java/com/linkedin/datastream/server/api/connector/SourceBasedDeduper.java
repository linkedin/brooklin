package com.linkedin.datastream.server.api.connector;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import org.apache.commons.lang.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.data.template.GetMode;
import com.linkedin.datastream.common.Datastream;
import com.linkedin.datastream.common.DatastreamMetadataConstants;
import com.linkedin.datastream.common.DatastreamRuntimeException;


/**
 * Deduper that uses the source connection string to figure out whether two datastreams can be de-duped.
 */
public class SourceBasedDeduper implements DatastreamDeduper {
  private static final String DEFAULT_TOPIC_REUSE = "true";

  private static final Logger LOG = LoggerFactory.getLogger(SourceBasedDeduper.class.getName());

  @Override
  public Optional<Datastream> findExistingDatastream(Datastream datastream, List<Datastream> allDatastream) {
    Validate.notNull(datastream, "Datastream should not be null");
    Validate.notNull(allDatastream, "Datastreams should not be null");
    Validate.isTrue(datastream.hasSource() && datastream.getSource().hasConnectionString() && !datastream.getSource()
        .getConnectionString()
        .isEmpty(), "Datastream source is either not present or empty.");
    Validate.isTrue(datastream.hasTransportProviderName() && datastream.hasConnectorName(),
        "Datastream doesnt' have transport or connector details set.");

    if (!isReusableDatastream(datastream)) {
      return Optional.empty();
    }

    if (datastream.hasDestination() && datastream.getDestination().hasConnectionString() &&
        !datastream.getDestination().getConnectionString().isEmpty()) {
      throw new DatastreamRuntimeException("Datastream already has destination populated.");
    }

    List<Datastream> duplicateDatastreams = allDatastream.stream()
        .filter(this::checkDatastreamSourceAndDestinationIsNotEmpty)
        .filter(this::isReusableDatastream)
        .filter(d -> checkEquivalentDatastreams(d, datastream))
        .collect(Collectors.toList());

    if (!duplicateDatastreams.isEmpty()) {
      LOG.info("Found duplicate datastreams {} for datastream {}", duplicateDatastreams, datastream);
    }

    return duplicateDatastreams.stream().findFirst();
  }

  private boolean checkEquivalentDatastreams(Datastream datastream1, Datastream datastream2) {
    return datastream1.getConnectorName().equals(datastream2.getConnectorName())
        && datastream1.getTransportProviderName().equals(datastream2.getTransportProviderName())
        && datastream1.getSource().getConnectionString().equals(datastream2.getSource().getConnectionString())
        && getKeySerDe(datastream1).equals(getKeySerDe(datastream2))
        && getPayloadSerDe(datastream1).equals(getPayloadSerDe(datastream2))
        && getEnvelopeSerDe(datastream1).equals(getEnvelopeSerDe(datastream2));
  }

  private Optional<String> getPayloadSerDe(Datastream datastream) {
    return Optional.ofNullable(datastream.getDestination().getPayloadSerDe(GetMode.NULL));
  }

  private Optional<String> getKeySerDe(Datastream datastream) {
    return Optional.ofNullable(datastream.getDestination().getKeySerDe(GetMode.NULL));
  }

  private Optional<String> getEnvelopeSerDe(Datastream datastream) {
    return Optional.ofNullable(datastream.getDestination().getEnvelopeSerDe(GetMode.NULL));
  }

  private boolean isReusableDatastream(Datastream datastream) {
    if (datastream.hasMetadata()) {
      return Boolean.parseBoolean(datastream.getMetadata()
          .getOrDefault(DatastreamMetadataConstants.REUSE_EXISTING_DESTINATION_KEY, DEFAULT_TOPIC_REUSE));
    } else {
      return Boolean.parseBoolean(DEFAULT_TOPIC_REUSE);
    }
  }

  private boolean checkDatastreamSourceAndDestinationIsNotEmpty(Datastream d) {
    return d.hasSource() && d.getSource().hasConnectionString() && !d.getSource().getConnectionString().isEmpty()
        && d.hasDestination() && d.getDestination().hasConnectionString() &&
        !d.getDestination().getConnectionString().isEmpty();
  }
}
