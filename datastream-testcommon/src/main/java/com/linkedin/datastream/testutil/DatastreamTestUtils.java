package com.linkedin.datastream.testutil;

import com.linkedin.data.template.StringMap;
import com.linkedin.datastream.common.Datastream;
import com.linkedin.datastream.common.DatastreamSource;
import com.linkedin.datastream.common.zk.ZkClient;
import com.linkedin.datastream.server.dms.ZookeeperBackedDatastreamStore;
import com.linkedin.datastream.server.zk.KeyBuilder;

import java.util.ArrayList;
import java.util.List;

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
      Datastream datastream = new Datastream();
      datastream.setName(datastreamName);
      datastream.setConnectorType(connectorType);
      datastream.setSource(new DatastreamSource());
      datastream.getSource().setConnectionString("sampleSource-" + ts + counter);
      datastream.setMetadata(new StringMap());
      datastreams.add(datastream);
      ++counter;
    }
    return datastreams.toArray(new Datastream[datastreams.size()]);
  }

  /**
   * Store the datastreams into the appropriate locations in zookeeper.
   * @param zkClient zookeeper client
   * @param cluster name of the datastream cluster
   * @param datastreams list of datastreams
   */
  public static void storeDatastreams(ZkClient zkClient, String cluster, Datastream... datastreams) {
    for (Datastream datastream : datastreams) {
      zkClient.ensurePath(KeyBuilder.datastreams(cluster));
      ZookeeperBackedDatastreamStore dsStore = new ZookeeperBackedDatastreamStore(zkClient, cluster);
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
   * @return
   */
  public static Datastream[] createAndStoreDatastreams(ZkClient zkClient, String cluster, String connectorType,
                                              String... datastreamNames) {
    Datastream[] datasteams = createDatastreams(connectorType, datastreamNames);
    storeDatastreams(zkClient, cluster, datasteams);
    return datasteams;
  }
}
