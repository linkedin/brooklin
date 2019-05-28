/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.connectors.kafka.mirrormaker;

import java.util.Collection;
import java.util.Collections;
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
 * A no-op implementation of {@link TopicManager}, used by default by mirror maker for topic management if no other
 * TopicManager is specified.
 */
public class NoOpTopicManager implements TopicManager {

  static final HashSet<TopicPartition> EMPTY_PARTITIONS_SET = new HashSet<>();

  /**
   * Constructor for NoOpTopicManager. Note that since its no-op, the constructor doesn't do anything.
   * @param datastreamTask Datastream task that the topic manager is being used inside.
   * @param datastream datastream that the datastream task is a part of.
   * @param groupIdConstructor Group ID constructor to use while generating consumer group name for any kafka consumers
   *                           inside the topic manager.
   * @param kafkaConsumerFactory Instance of KafkaConsumerFactory to use for creating any consumer within topic manager.
   * @param consumerProperties Properties to be applied for any consumer that is created within topic manager.
   * @param topicManagerProperties Properties of topic manager itself.
   * @param consumerMetrics Instance of CommonConnectorMetrics to report any metrics.
   * @param metricsPrefix Prefix to use to create any metrics inside topic manager.
   * @param metricsKey Metrics key to use to create any metrics inside topic manager.
   */
  public NoOpTopicManager(DatastreamTask datastreamTask, Datastream datastream, GroupIdConstructor groupIdConstructor,
      KafkaConsumerFactory<?, ?> kafkaConsumerFactory, Properties consumerProperties, Properties topicManagerProperties,
      CommonConnectorMetrics consumerMetrics, String metricsPrefix, String metricsKey) {
  }

  /**
   * This method is called during a kafka rebalance, within kafka's onPartitionsAssigned callback. This is a no-op for
   * this topic manager.
   * @param partitions Partitions which were assigned.
   */
  public Collection<TopicPartition> onPartitionsAssigned(Collection<TopicPartition> partitions) {
    return Collections.unmodifiableSet(EMPTY_PARTITIONS_SET);
  }

  /**
   * This method is called during a kafka rebalance, within kafka's onPartitionsRevoked callback. This is a no-op for
   * this topic manager.
   * @param partitions Partitions which were revoked.
   */
  public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
  }

  /**
   * Check if a partition that topic manager has paused should resume. It throws exception for this topic manager
   * as its a no-op topic manager; it is not expected to pause any partition to begin with.
   * @param partition The partition to check whether to resume.
   * @return
   */
  public boolean shouldResumePartition(TopicPartition partition) {
    // This should not happen, as this class's implementation of onPartitionsAssigned() returns an empty set.
    throw new DatastreamRuntimeException("shouldResumePartition called in NoOpTopicManager for partition : " + partition);
  }

  /**
   * Stop topic manager. This is a no-op for this topic manager.
   */
  public void stop() {
  }
}
