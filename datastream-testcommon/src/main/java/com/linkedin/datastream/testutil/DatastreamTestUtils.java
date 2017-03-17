package com.linkedin.datastream.testutil;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import com.linkedin.data.template.StringMap;
import com.linkedin.datastream.common.Datastream;
import com.linkedin.datastream.common.DatastreamDestination;
import com.linkedin.datastream.common.DatastreamException;
import com.linkedin.datastream.common.DatastreamMetadataConstants;
import com.linkedin.datastream.common.DatastreamSource;
import com.linkedin.datastream.common.DatastreamStatus;
import com.linkedin.datastream.common.zk.ZkClient;
import com.linkedin.datastream.server.CachedDatastreamReader;
import com.linkedin.datastream.server.DatastreamTaskImpl;
import com.linkedin.datastream.server.DummyTransportProviderAdminFactory;
import com.linkedin.datastream.server.dms.ZookeeperBackedDatastreamStore;
import com.linkedin.datastream.server.zk.KeyBuilder;


/**
 * Utility class for writing tests that deal with Datastream objects.
 */
public class DatastreamTestUtils {
  /**
   * Creates variable number of Datastreams from a list of names with
   * fields populated with default values except for name and connector type.
   * Caller can modify the values later on to more meaning values.
   *
   * @param connectorType connector type string
   * @param datastreamNames list of datastream names
   * @return an array of well-formed Datastreams
   */
  public static Datastream[] createDatastreams(String connectorType, String... datastreamNames) {
    List<Datastream> datastreams = new ArrayList<>();
    Integer counter = 0;
    String ts = String.valueOf(System.currentTimeMillis());
    for (String datastreamName : datastreamNames) {
      Datastream datastream = createDatastream(connectorType, datastreamName, "sampleSource-" + ts + counter,
          "sampleDestination-" + ts + counter, 1);
      datastreams.add(datastream);
      ++counter;
    }
    return datastreams.toArray(new Datastream[datastreams.size()]);
  }

  /**
   * Creates a test datastream of specific connector type
   * @param connectorType connector type of the datastream to be created.
   * @param datastreamName Name of the datastream to be created.
   * @param source source connection string to be used.
   * @return Datastream that is created.
   */
  public static Datastream createDatastream(String connectorType, String datastreamName, String source) {
    Datastream ds = new Datastream();
    ds.setName(datastreamName);
    ds.setConnectorName(connectorType);
    ds.setSource(new DatastreamSource());
    ds.getSource().setConnectionString(source);
    StringMap metadata = new StringMap();
    metadata.put(DatastreamMetadataConstants.OWNER_KEY, "dummy_owner");
    ds.setMetadata(metadata);
    ds.setDestination(new DatastreamDestination());
    ds.setTransportProviderName(DummyTransportProviderAdminFactory.PROVIDER_NAME);
    return ds;
  }

  /**
   * Creates a test datastream of specific connector type
   * @param connectorType connector type of the datastream to be created.
   * @param datastreamName Name of the datastream to be created.
   * @param source source connection string to be used.
   * @param destination the destination connection string to be used.
   * @param destinationPartitions the destination partitions
   * @return Datastream that is created.
   */
  public static Datastream createDatastream(String connectorType, String datastreamName, String source,
      String destination, int destinationPartitions) {
    Datastream ds = new Datastream();
    ds.setName(datastreamName);
    ds.setConnectorName(connectorType);
    ds.setSource(new DatastreamSource());
    ds.getSource().setConnectionString(source);
    ds.setDestination(new DatastreamDestination());
    ds.setTransportProviderName(DummyTransportProviderAdminFactory.PROVIDER_NAME);
    ds.getDestination().setConnectionString(destination);
    ds.getDestination().setPartitions(destinationPartitions);
    ds.setStatus(DatastreamStatus.INITIALIZING);
    StringMap metadata = new StringMap();
    metadata.put(DatastreamMetadataConstants.OWNER_KEY, "dummy_owner");
    metadata.put(DatastreamMetadataConstants.CREATION_MS, String.valueOf(Instant.now().toEpochMilli()));
    ds.setMetadata(metadata);
    return ds;
  }

  /**
   * Store the datastreams into the appropriate locations in zookeeper.
   * @param zkClient zookeeper client
   * @param cluster name of the datastream cluster
   * @param datastreams list of datastreams
   * @throws DatastreamException the datastream exception
   */
  public static void storeDatastreams(ZkClient zkClient, String cluster, Datastream... datastreams)
      throws DatastreamException {
    for (Datastream datastream : datastreams) {
      zkClient.ensurePath(KeyBuilder.datastreams(cluster));
      CachedDatastreamReader datastreamCache = new CachedDatastreamReader(zkClient, cluster);
      ZookeeperBackedDatastreamStore dsStore = new ZookeeperBackedDatastreamStore(datastreamCache, zkClient, cluster);
      dsStore.createDatastream(datastream.getName(), datastream);
    }
  }

  /**
   * Create a list of Datastreams with default fields and store them into ZooKeeper.
   * This can be used when the test does not need to modify the default fields.
   * @param zkClient zookeeper client
   * @param cluster name of the datastream cluster
   * @param connectorType connector type string
   * @param datastreamNames list of datastream names
   * @return datastream [ ]
   * @throws DatastreamException the datastream exception
   */
  public static Datastream[] createAndStoreDatastreams(ZkClient zkClient, String cluster, String connectorType,
      String... datastreamNames) throws DatastreamException {
    Datastream[] datastreams = createDatastreams(connectorType, datastreamNames);
    for (Datastream ds : datastreams) {
      ds.getMetadata().put(DatastreamMetadataConstants.TASK_PREFIX, DatastreamTaskImpl.getTaskPrefix(ds));
    }
    storeDatastreams(zkClient, cluster, datastreams);
    return datastreams;
  }
}
