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
  public TopicManager createTopicManager(DatastreamTask datastreamTask, Datastream datastream,
      GroupIdConstructor groupIdConstructor, KafkaConsumerFactory<?, ?> kafkaConsumerFactory,
      Properties consumerProperties, Properties topicManagerProperties, CommonConnectorMetrics consumerMetrics) {
    return new NoOpTopicManager(datastreamTask, datastream, groupIdConstructor, kafkaConsumerFactory, consumerProperties,
        topicManagerProperties, consumerMetrics);
  }
}
