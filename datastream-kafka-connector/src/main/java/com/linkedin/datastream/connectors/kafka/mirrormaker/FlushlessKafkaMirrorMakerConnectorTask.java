package com.linkedin.datastream.connectors.kafka.mirrormaker;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.datastream.common.DatastreamConstants;
import com.linkedin.datastream.connectors.kafka.KafkaBasedConnectorConfig;
import com.linkedin.datastream.connectors.kafka.PausedSourcePartitionMetadata;
import com.linkedin.datastream.server.DatastreamProducerRecord;
import com.linkedin.datastream.server.DatastreamTask;
import com.linkedin.datastream.server.FlushlessEventProducerHandler;


/**
 * Package-private class that extends from {@link KafkaMirrorMakerConnectorTask} to override methods involved in
 * flushless producing of events. This class will be instantiated if the KafkaMirrorMakerConnector config has
 * isFlushlessModeEnabled set to true. Otherwise, the KafkaMirrorMakerConnectorTask (which does occasional flush) will
 * be used. Note that this FlushlessKafkaMirrorMakerConnectorTask does do flush on hard-commit, whenever a graceful
 * shutdown occurs.
 */
class FlushlessKafkaMirrorMakerConnectorTask extends KafkaMirrorMakerConnectorTask {

  // Use the KafkaMirrorMakerConnectorTask logger
  private static final Logger LOG = LoggerFactory.getLogger(KafkaMirrorMakerConnectorTask.class.getName());

  protected static final String CONFIG_MAX_IN_FLIGHT_MSGS_THRESHOLD = "maxInFlightMessagesThreshold";
  protected static final String CONFIG_MIN_IN_FLIGHT_MSGS_THRESHOLD = "minInFlightMessagesThreshold";
  protected static final long DEFAULT_MAX_IN_FLIGHT_MSGS_THRESHOLD = 5000;
  protected static final long DEFAULT_MIN_IN_FLIGHT_MSGS_THRESHOLD = 1000;

  private final FlushlessEventProducerHandler<Long> _flushlessProducer;

  private final long _maxInFlightMessagesThreshold;
  private final long _minInFlightMessagesThreshold;

  protected FlushlessKafkaMirrorMakerConnectorTask(KafkaBasedConnectorConfig config, DatastreamTask task) {
    super(config, task);
    _flushlessProducer = new FlushlessEventProducerHandler<>(_producer);
    _maxInFlightMessagesThreshold =
        config.getConnectorProps().getLong(CONFIG_MAX_IN_FLIGHT_MSGS_THRESHOLD, DEFAULT_MAX_IN_FLIGHT_MSGS_THRESHOLD);
    _minInFlightMessagesThreshold =
        config.getConnectorProps().getLong(CONFIG_MIN_IN_FLIGHT_MSGS_THRESHOLD, DEFAULT_MIN_IN_FLIGHT_MSGS_THRESHOLD);
  }

  @Override
  protected void sendDatastreamProducerRecord(DatastreamProducerRecord datastreamProducerRecord) throws Exception {
    KafkaMirrorMakerCheckpoint sourceCheckpoint =
        new KafkaMirrorMakerCheckpoint(datastreamProducerRecord.getCheckpoint());
    String topic = sourceCheckpoint.getTopic();
    int partition = sourceCheckpoint.getPartition();
    _flushlessProducer.send(datastreamProducerRecord, topic, partition, sourceCheckpoint.getOffset());
    TopicPartition tp = new TopicPartition(topic, partition);
    if (_flushlessProducer.getInFlightCount(topic, partition) > _maxInFlightMessagesThreshold) {
      // add the partition to the pause list
      _autoPausedSourcePartitions.put(tp, new PausedSourcePartitionMetadata(
          () -> _flushlessProducer.getInFlightCount(topic, partition) <= _minInFlightMessagesThreshold,
          PausedSourcePartitionMetadata.Reason.EXCEEDED_MAX_IN_FLIGHT_MSG_THRESHOLD));
      _taskUpdates.add(DatastreamConstants.UpdateType.PAUSE_RESUME_PARTITIONS);
    }
  }

  @Override
  protected void maybeCommitOffsets(Consumer<?, ?> consumer, boolean hardCommit) {
    boolean isTimeToCommit = System.currentTimeMillis() - _lastCommittedTime > _offsetCommitInterval;

    if (hardCommit) { // hard commit (flush and commit checkpoints)
      LOG.info("Calling flush on the producer.");
      _datastreamTask.getEventProducer().flush();
      consumer.commitSync();
      // verify that the producer is caught up with the consumer, since flush was called
      for (TopicPartition tp : consumer.assignment()) {
        _flushlessProducer.getAckCheckpoint(tp.topic(), tp.partition()).ifPresent(ackCheckpoint -> {
          long committedCheckpoint =
              Optional.ofNullable(consumer.committed(tp)).map(OffsetAndMetadata::offset).orElse(0L);
          if (!Objects.equals(ackCheckpoint, committedCheckpoint)) {
            LOG.error(
                "Ack checkpoint should match committed checkpoint after flushing and checkpointing. Ack checkpoint: "
                    + "{}, consumer position: {}", ackCheckpoint, committedCheckpoint);
          }
        });

        // verify that the in-flight count is 0 after flush
        long inFlightCount = _flushlessProducer.getInFlightCount(tp.topic(), tp.partition());
        if (inFlightCount > 0) {
          LOG.error("Flushless producer inflight count for topic {} partition {} should be 0 after flush, but was {}",
              tp.topic(), tp.partition(), inFlightCount);
        }
      }
      // clear the flushless producer state after flushing all messages and checkpointing
      _flushlessProducer.clear();
      _lastCommittedTime = System.currentTimeMillis();

    } else if (isTimeToCommit) { // soft commit (no flush, just commit checkpoints)
      LOG.info("Trying to commit offsets.");
      Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
      for (TopicPartition tp : consumer.assignment()) {
        // add 1 to the last acked checkpoint to set to the offset of the next message to consume
        _flushlessProducer.getAckCheckpoint(tp.topic(), tp.partition())
            .ifPresent(o -> offsets.put(tp, new OffsetAndMetadata(o + 1)));
      }
      consumer.commitSync(offsets);
      _lastCommittedTime = System.currentTimeMillis();
    }
  }
}

