/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.connectors.kafka;

import java.util.Properties;

import com.linkedin.datastream.common.zk.ZkClient;
import com.linkedin.datastream.kafka.KafkaTransportProviderAdmin;
import com.linkedin.datastream.server.CachedDatastreamReader;
import com.linkedin.datastream.server.Coordinator;
import com.linkedin.datastream.server.CoordinatorConfig;
import com.linkedin.datastream.server.DummyTransportProviderAdminFactory;
import com.linkedin.datastream.testutil.DatastreamEmbeddedZookeeperKafkaCluster;


/**
 * Utility methods for {@link KafkaConnector} tests
 */
public class TestKafkaConnectorUtils {
  /**
   * Create a Coordinator
   * @param zkAddr ZooKeeper server address
   * @param cluster Brooklin cluster name
   */
  public static Coordinator createCoordinator(String zkAddr, String cluster) throws Exception {
    return createCoordinator(zkAddr, cluster, new Properties());
  }

  /**
   * Create a Coordinator
   * @param zkAddr ZooKeeper server address
   * @param cluster Brooklin cluster name
   * @param override Config properties to use
   */
  public static Coordinator createCoordinator(String zkAddr, String cluster, Properties override) throws Exception {
    Properties props = new Properties();
    props.put(CoordinatorConfig.CONFIG_CLUSTER, cluster);
    props.put(CoordinatorConfig.CONFIG_ZK_ADDRESS, zkAddr);
    props.put(CoordinatorConfig.CONFIG_ZK_SESSION_TIMEOUT, String.valueOf(ZkClient.DEFAULT_SESSION_TIMEOUT));
    props.put(CoordinatorConfig.CONFIG_ZK_CONNECTION_TIMEOUT, String.valueOf(ZkClient.DEFAULT_CONNECTION_TIMEOUT));
    props.putAll(override);
    ZkClient client = new ZkClient(zkAddr);
    CachedDatastreamReader cachedDatastreamReader = new CachedDatastreamReader(client, cluster);
    Coordinator coordinator = new Coordinator(cachedDatastreamReader, props);
    DummyTransportProviderAdminFactory factory = new DummyTransportProviderAdminFactory();
    coordinator.addTransportProvider(DummyTransportProviderAdminFactory.PROVIDER_NAME,
        factory.createTransportProviderAdmin(DummyTransportProviderAdminFactory.PROVIDER_NAME, new Properties()));
    return coordinator;
  }

  /**
   * Create a Kafka transport provider admin for an embedded Kafka cluster
   */
  public static KafkaTransportProviderAdmin<byte[], byte[]> createKafkaTransportProviderAdmin(DatastreamEmbeddedZookeeperKafkaCluster kafkaCluster) {
    Properties props = new Properties();
    props.put("zookeeper.connect", kafkaCluster.getZkConnection());
    props.put("bootstrap.servers", kafkaCluster.getBrokers());
    return new KafkaTransportProviderAdmin<>("test", props);
  }
}
