package com.linkedin.datastream.server;

import java.util.HashMap;
import java.util.List;
import java.util.Properties;
import java.util.stream.Stream;

import com.linkedin.datastream.common.Datastream;
import com.linkedin.datastream.common.VerifiableProperties;


/**
 * Class that uses the Transport provider to manage the topics used by the datastream
 */
public class DatastreamDestinationManager {

  private final TransportProvider _transportProvider;
  private final int DEFAULT_NUMBER_PARTITIONS = 1;

  public DatastreamDestinationManager(TransportProvider transportProvider) {
    _transportProvider = transportProvider;
  }

  /**
   *
   * @param datastreams
   */
  public void populateDatastreamDestination(List<Datastream> datastreams) {
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
   *
   * @param datastream
   * @param allDatastreams
   */
  public void deleteDatastreamDestination(Datastream datastream, List<Datastream> allDatastreams) {
    Stream<Datastream> duplicateDatastreams = allDatastreams.stream().filter(d ->
        d.getDestination().equals(datastream.getDestination()) && !d.getName().equalsIgnoreCase(datastream.getName()));

    // If there are no datastreams using the same destination, then delete the topic.
    if(duplicateDatastreams.count() == 0) {
      _transportProvider.dropTopic(datastream.getDestination());
    }
  }
}
