/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.connectors.kafka.mirrormaker;

import java.util.Properties;

import com.linkedin.datastream.common.Datastream;
import com.linkedin.datastream.connectors.CommonConnectorMetrics;
import com.linkedin.datastream.connectors.kafka.GroupIdConstructor;
import com.linkedin.datastream.connectors.kafka.KafkaConsumerFactory;
import com.linkedin.datastream.server.DatastreamTask;


/**
 * This class implements TopicManagerFactory interface and returns NoOpTopicManager instance. This factory is used by default
 * by mirror maker to create topic manager instance.
 */
public class NoOpTopicManagerFactory implements TopicManagerFactory {

  /**
   * Method to create an instance of NoOpTopicManager.
   * @param datastreamTask Task that topic manager is going to be created for.
   * @param datastream Datastream that topic manager is going to be created for.
   * @param groupIdConstructor This will be used while creating source/destination consumers
   * @param kafkaConsumerFactory This will be used to create source/destination Kafka consumer
   * @param consumerProperties properties for Kafka consumer
   * @param topicManagerProperties Any additional properties that need to be passed to topic manager
   * @param consumerMetrics In case one needs to log consumer metrics.
   * @param metricsPrefix Prefix to use for metrics that topic manager emits.
   * @param metricsKey Key to use for metrics that topic manager emits
   * @return Instance of NoOpTopicManager.
   */
  public TopicManager createTopicManager(DatastreamTask datastreamTask, Datastream datastream,
      GroupIdConstructor groupIdConstructor, KafkaConsumerFactory<?, ?> kafkaConsumerFactory,
      Properties consumerProperties, Properties topicManagerProperties, CommonConnectorMetrics consumerMetrics,
      String metricsPrefix, String metricsKey) {
    return new NoOpTopicManager(datastreamTask, datastream, groupIdConstructor, kafkaConsumerFactory,
        consumerProperties, topicManagerProperties, consumerMetrics, metricsPrefix, metricsKey);
  }
}
