package com.linkedin.datastream.connectors.kafka.mirrormaker;

import java.util.Collection;
import org.apache.kafka.common.TopicPartition;

/**
 * The interface can used to implement any topics related management that mirror maker needs to do (for example,
 */
public interface TopicManager {

  /**
   * This gets called as part of Kafka's onPartitionsAssigned() callback, in case any topic management needs to be done.
   * @return List of topic partitions that need to be paused as result of topic management
   * (for example, in case topic couldn't be created on destination side).
   */
  public Collection<TopicPartition> onPartitionsAssigned(Collection<TopicPartition> partitions);

  /**
   * This gets called as part of Kafka's onPartitionsRevoked() callback, in case any topic management needs to be done
   * when partitions are revoked by kafka.
   */
  public void onPartitionsRevoked(Collection<TopicPartition> partitions);

  /**
   * Method that mirror maker calls to check if a partition that Topic Manager deemed should be paused previously
   * should now be unpaused.
   * @return true if partition should be unpaused, false otherwise.
   */
  public boolean shoudResumePartition(TopicPartition tp);
}
