package com.linkedin.datastream.server;

import java.util.HashMap;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.stream.Stream;

import com.linkedin.datastream.common.Datastream;
import com.linkedin.datastream.common.VerifiableProperties;


/**
 * Class that uses the Transport provider to manage the topics used by the datastream
 */
public class TopicManager {

  private final TransportProvider _transportProvider;
  private final int DEFAULT_NUMBER_PARTITIONS = 1;

  public TopicManager(TransportProvider transportProvider) {
    _transportProvider = transportProvider;
  }

  /**
   * populates the datastream destination for the newly created datastreams.
   * Caller (Datastream leader) should pass in all the datastreams present in the system.
   * This method will take care of de-duping the datastreams, i.e. if there is an existing
   * datastream with the same source, they will use the same destination.
   * @param datastreams All datastreams in the current system.
   */
  public void populateDatastreamDestination(List<Datastream> datastreams) {
    Objects.requireNonNull(datastreams, "Datastream should not be null");

    HashMap<String, String> sourceDestinationMapping = new HashMap<>();
    datastreams.stream().filter(d -> !d.getDestination().isEmpty())
        .forEach(d -> sourceDestinationMapping.put(d.getSource(), d.getDestination()));

    datastreams.stream().filter(d -> d.getDestination().isEmpty() && sourceDestinationMapping.containsKey(d.getSource()))
        .forEach(d -> d.setDestination(sourceDestinationMapping.get(d.getSource())));

    datastreams.stream().filter(d -> d.getDestination().isEmpty())
        .forEach(d -> {
          if (sourceDestinationMapping.containsKey(d.getSource())) {
            d.setDestination(sourceDestinationMapping.get(d.getSource()));
          } else {
            String destination = createTopic(d);
            sourceDestinationMapping.put(d.getSource(), destination);
            d.setDestination(destination);
          }
        });

  }

  private String createTopic(Datastream datastream) {
    Properties datastreamProperties = new Properties();
    datastreamProperties.putAll(datastream.getMetadata());
    Properties topicProperties = new VerifiableProperties(datastreamProperties).getDomainProperties("topic");
    return _transportProvider.createTopic(datastream.getName(), DEFAULT_NUMBER_PARTITIONS, topicProperties);
  }

  /**
   * Delete the datastream destination for a particular datastream.
   * Caller should pass in all the datastreams present in the system.
   * This method will ensure that there are no other references to the destination before deleting it.
   * @param datastream Datastream whose destination needs to be deleted.
   * @param allDatastreams All the datastreams in the system.
   */
  public void deleteDatastreamDestination(Datastream datastream, List<Datastream> allDatastreams) {
    Objects.requireNonNull(datastream, "Datastream should not be null");
    Objects.requireNonNull(allDatastreams, "allDatastreams should not be null");
    Stream<Datastream> duplicateDatastreams = allDatastreams.stream().filter(d ->
        d.getDestination().equals(datastream.getDestination()) && !d.getName().equalsIgnoreCase(datastream.getName()));

    // If there are no datastreams using the same destination, then delete the topic.
    if(duplicateDatastreams.count() == 0) {
      _transportProvider.dropTopic(datastream.getDestination());
    }
  }
}
