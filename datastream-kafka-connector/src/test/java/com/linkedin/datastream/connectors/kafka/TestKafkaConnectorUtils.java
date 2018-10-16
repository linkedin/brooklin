package com.linkedin.datastream.connectors.kafka;

import com.linkedin.datastream.kafka.DatastreamEmbeddedZookeeperKafkaCluster;
import com.linkedin.datastream.kafka.KafkaTransportProviderAdmin;
import java.util.Properties;

import com.linkedin.datastream.common.zk.ZkClient;
import com.linkedin.datastream.server.CachedDatastreamReader;
import com.linkedin.datastream.server.Coordinator;
import com.linkedin.datastream.server.CoordinatorConfig;
import com.linkedin.datastream.server.DummyTransportProviderAdminFactory;


public class TestKafkaConnectorUtils {
  public static Coordinator createCoordinator(String zkAddr, String cluster) throws Exception {
    return createCoordinator(zkAddr, cluster, new Properties());
  }

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

  public static KafkaTransportProviderAdmin getKafkaTransportProviderAdmin(DatastreamEmbeddedZookeeperKafkaCluster kafkaCluster) {
    Properties props = new Properties();
    props.put("zookeeper.connect", kafkaCluster.getZkConnection());
    props.put("bootstrap.servers", kafkaCluster.getBrokers());
    return new KafkaTransportProviderAdmin(props);
  }
}
