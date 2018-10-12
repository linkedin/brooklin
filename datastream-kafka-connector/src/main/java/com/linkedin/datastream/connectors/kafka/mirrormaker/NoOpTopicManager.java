package com.linkedin.datastream.connectors.kafka.mirrormaker;

import java.util.Collection;
import java.util.HashSet;

import java.util.Properties;
import org.apache.kafka.common.TopicPartition;

import com.linkedin.datastream.common.Datastream;
import com.linkedin.datastream.common.DatastreamRuntimeException;
import com.linkedin.datastream.connectors.CommonConnectorMetrics;
import com.linkedin.datastream.connectors.kafka.GroupIdConstructor;
import com.linkedin.datastream.connectors.kafka.KafkaConsumerFactory;
import com.linkedin.datastream.server.DatastreamTask;


/**
 * The class implements a no-op version of TopicManager interface. This class is used by default by mirror maker for topic
 * management, if no other TopicManager is specified.
 */
public class NoOpTopicManager implements TopicManager {

  public NoOpTopicManager(DatastreamTask datastreamTask, Datastream datastream, GroupIdConstructor groupIdConstructor,
      KafkaConsumerFactory<?, ?> kafkaConsumerFactory, Properties properties, CommonConnectorMetrics consumerMetrics) {
  }

  public Collection<TopicPartition> onPartitionsAssigned(Collection<TopicPartition> partitions) {
    return new HashSet<>();
  }

  public void onPartitionsRevoked(Collection<TopicPartition> partitions) {

  }

  public boolean shoudResumePartition(TopicPartition tp) {
    // This should not happen, as onPartitionsAssigned doesn't return any partitions to pause in onPartitionsAssigned().
    throw new DatastreamRuntimeException("shoudResumePartition called in NoOpTopicManager for partition : " + tp);
  }
}
