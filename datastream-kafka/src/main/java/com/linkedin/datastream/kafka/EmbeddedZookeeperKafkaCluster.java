package com.linkedin.datastream.kafka;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;


public class EmbeddedZookeeperKafkaCluster implements KafkaCluster {
  private EmbeddedZookeeper _embeddedZookeeper = null;
  private EmbeddedKafkaCluster _embeddedKafkaCluster = null;
  private boolean _isStarted;

  public EmbeddedZookeeperKafkaCluster() {
    _embeddedZookeeper = new EmbeddedZookeeper(-1);
    List<Integer> kafkaPorts = new ArrayList<>();
    // -1 for any available port
    kafkaPorts.add(-1);
    kafkaPorts.add(-1);
    _embeddedKafkaCluster = new EmbeddedKafkaCluster(_embeddedZookeeper.getConnection(), new Properties(), kafkaPorts);
    _isStarted = false;
  }

  @Override
  public String getBrokers() {
    return _embeddedKafkaCluster.getBrokers();
  }

  @Override
  public String getZkConnection() {
    return _embeddedKafkaCluster.getZkConnection();
  }

  @Override
  public boolean isStarted() {
    return _isStarted;
  }

  @Override
  public void startup() {
    try {
      _embeddedZookeeper.startup();
    } catch (IOException e) {
      throw new RuntimeException("Starting zookeeper failed with exception", e);
    }
    _embeddedKafkaCluster.startup();
    _isStarted = true;
  }

  @Override
  public void shutdown() {
    _embeddedKafkaCluster.shutdown();
    _embeddedZookeeper.shutdown();
    _isStarted = false;
  }
}
