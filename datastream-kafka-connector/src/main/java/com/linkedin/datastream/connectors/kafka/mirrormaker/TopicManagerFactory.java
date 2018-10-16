package com.linkedin.datastream.connectors.kafka.mirrormaker;

import java.util.Properties;

import com.linkedin.datastream.common.Datastream;
import com.linkedin.datastream.connectors.CommonConnectorMetrics;
import com.linkedin.datastream.connectors.kafka.GroupIdConstructor;
import com.linkedin.datastream.server.DatastreamTask;
import com.linkedin.datastream.connectors.kafka.KafkaConsumerFactory;

/**
 * Interface that is used to create topic manager.
 */
public interface TopicManagerFactory {
  /**
   * Method to create topic manager instance.
   * @param datastreamTask Task that topic manager is going to be created for.
   * @param datastream Datastream that topic manager is going to be created for.
   * @param groupIdConstructor This will be used while creating source/destination consumers
   * @param kafkaConsumerFactory This will be used to create source/destination kafka consumer
   * @param properties Any additional properties that need to be passed to topic manager
   * @param consumerMetrics In case one needs to log consumer metrics.
   * @return An instance of a class that implements TopicManager interface.
   */
  TopicManager createTopicManager(DatastreamTask datastreamTask, Datastream datastream,
      GroupIdConstructor groupIdConstructor, KafkaConsumerFactory<?, ?> kafkaConsumerFactory, Properties properties, CommonConnectorMetrics consumerMetrics);
}
