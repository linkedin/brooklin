/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.server;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.linkedin.datastream.common.BrooklinEnvelope;
import com.linkedin.datastream.common.ReflectionUtils;
import com.linkedin.datastream.server.api.transport.DatastreamRecordMetadata;
import com.linkedin.datastream.server.api.transport.SendCallback;
import com.linkedin.datastream.server.callbackstatus.CallbackStatusFactory;
import com.linkedin.datastream.server.callbackstatus.CallbackStatusWithComparableOffsetsFactory;
import com.linkedin.datastream.server.callbackstatus.CallbackStatusWithNonComparableOffsetsFactory;

import static com.linkedin.datastream.server.FlushlessEventProducerHandler.SourcePartition;


/**
 * Tests for {@link FlushlessEventProducerHandler}
 */
public class TestFlushlessEventProducerHandler {
  private static final Long BIG_CHECKPOINT = Long.MAX_VALUE;
  private static final String TOPIC = "MyTopic";
  private static final Random RANDOM = new Random();

  private static final CallbackStatusFactory<Long> OFFSET_CHECKPOINT_TRACKING_STRATEGY_WITH_NON_COMPARABLE_OFFSETS =
      ReflectionUtils.createInstance(CallbackStatusWithNonComparableOffsetsFactory.class.getName());

  private static final CallbackStatusFactory<Long> OFFSET_CHECKPOINT_TRACKING_STRATEGY_WITH_COMPARABLE_OFFSETS =
      ReflectionUtils.createInstance(CallbackStatusWithComparableOffsetsFactory.class.getName());

  /**
   * Helper function to test the scenario of sending single record for both comparable and non comparable offsets
   * @param eventProducer is the interface for Connectors to send events to the designated destination
   * @param handler is the flushless producer handler that does offset checkpoint management
   */
  private void testSingleRecordWithGivenHandler(DatastreamEventProducer eventProducer, FlushlessEventProducerHandler<Long> handler) {
    long checkpoint = 1;
    DatastreamProducerRecord record = getDatastreamProducerRecord(checkpoint, TOPIC, 1);

    Assert.assertEquals(handler.getAckCheckpoint(BIG_CHECKPOINT, Comparator.naturalOrder()).get(), BIG_CHECKPOINT);
    handler.send(record, TOPIC, 1, checkpoint, null);
    Assert.assertEquals(handler.getAckCheckpoint(BIG_CHECKPOINT, Comparator.naturalOrder()), Optional.empty());
    Assert.assertEquals(handler.getInFlightCount(TOPIC, 1), 1);
    Assert.assertEquals(handler.getAckCheckpoint(TOPIC, 1), Optional.empty());
    Assert.assertEquals(handler.getAckMessagesPastCheckpointCount(TOPIC, 1), 0);
    eventProducer.flush();
    Assert.assertEquals(handler.getAckCheckpoint(BIG_CHECKPOINT, Comparator.naturalOrder()).get(), BIG_CHECKPOINT);
    Assert.assertEquals(handler.getInFlightCount(TOPIC, 1), 0);
    Assert.assertEquals(handler.getAckCheckpoint(TOPIC, 1).get(), new Long(checkpoint));
    Assert.assertEquals(handler.getAckMessagesPastCheckpointCount(TOPIC, 1), 0);
  }

  @Test
  public void testSingleRecordWithComparableOffsets() {
    RandomEventProducer eventProducer = new RandomEventProducer();
    FlushlessEventProducerHandler<Long> handler =
        new FlushlessEventProducerHandler<>(eventProducer, OFFSET_CHECKPOINT_TRACKING_STRATEGY_WITH_COMPARABLE_OFFSETS);
    testSingleRecordWithGivenHandler(eventProducer, handler);
  }

  @Test
  public void testSingleRecordWithNonComparableOffsets() {
    RandomEventProducer eventProducer = new RandomEventProducer();
    FlushlessEventProducerHandler<Long> handler = new FlushlessEventProducerHandler<>(eventProducer,
        OFFSET_CHECKPOINT_TRACKING_STRATEGY_WITH_NON_COMPARABLE_OFFSETS);
    testSingleRecordWithGivenHandler(eventProducer, handler);
  }

  @Test
  public void testMultipleSendsWithComparableOffsets() {
    RandomEventProducer eventProducer = new RandomEventProducer();
    FlushlessEventProducerHandler<Long> handler =
        new FlushlessEventProducerHandler<>(eventProducer, OFFSET_CHECKPOINT_TRACKING_STRATEGY_WITH_COMPARABLE_OFFSETS);

    // Send 1000 messages to 10 partitions
    for (int i = 0; i < 10; i++) {
      SourcePartition tp = new SourcePartition(TOPIC, i);
      for (int j = 0; j < 100; j++) {
        sendEvent(tp, handler, j);
      }
    }

    for (int i = 0; i < 999; i++) {
      eventProducer.processOne();
      long minOffsetPending = eventProducer.minCheckpoint();
      Long ackOffset = handler.getAckCheckpoint(BIG_CHECKPOINT, Comparator.naturalOrder()).orElse(-1L);

      Assert.assertTrue(ackOffset < minOffsetPending,
          "Not true that " + ackOffset + " is less than " + minOffsetPending);
    }
    // event producer calls ack on the last element
    eventProducer.processOne();

    for (int par = 0; par < 10; par++) {
      Assert.assertEquals(handler.getInFlightCount(TOPIC, par), 0);
    }

    Assert.assertEquals(handler.getAckCheckpoint(BIG_CHECKPOINT, Comparator.naturalOrder()).get(), BIG_CHECKPOINT);

    Assert.assertEquals(handler.getAckCheckpoint(BIG_CHECKPOINT, Comparator.naturalOrder()).get(), BIG_CHECKPOINT);
  }

  @Test
  public void testMultipleSendsWithNonComparableOffsets() {
    RandomEventProducer eventProducer = new RandomEventProducer();
    FlushlessEventProducerHandler<Long> handler =
        new FlushlessEventProducerHandler<>(eventProducer, OFFSET_CHECKPOINT_TRACKING_STRATEGY_WITH_NON_COMPARABLE_OFFSETS);

    // Send 1000 messages to 10 partitions
    for (int i = 0; i < 10; i++) {
      SourcePartition tp = new SourcePartition(TOPIC, i);
      for (int j = 0; j < 100; j++) {
        sendEvent(tp, handler, j);
      }
    }

    for (int i = 0; i < 1000; i++) {
      eventProducer.processOne();
    }

    for (int par = 0; par < 10; par++) {
      Assert.assertEquals(handler.getInFlightCount(TOPIC, par), 0);
    }
  }

  /**
   * Helper function to test the scenario of sending multiple records for both comparable and non comparable offsets
   * and validating acking behaviors
   * @param eventProducer is the interface for Connectors to send events to the designated destination
   * @param handler is the flushless producer handler that does offset checkpoint management
   */
  private void testOutOfOrderAckForGivenHandler(RandomEventProducer eventProducer, FlushlessEventProducerHandler<Long> handler) {
    int partition = 0;
    SourcePartition tp = new SourcePartition(TOPIC, partition);

    // Send 5 messages to partition 0 with increasing checkpoints (0-4)
    for (int i = 0; i < 5; i++) {
      sendEvent(tp, handler, i);
    }

    // simulate callback for checkpoint 4
    eventProducer.process(tp, 4); // inflight result: 0, 1, 2, 3
    Assert.assertEquals(handler.getAckCheckpoint(TOPIC, partition), Optional.empty(),
        "Safe checkpoint should be empty");
    Assert.assertEquals(handler.getInFlightCount(TOPIC, partition), 4, "Number of inflight messages should be 4");
    Assert.assertEquals(handler.getAckMessagesPastCheckpointCount(TOPIC, partition), 1);

    // simulate callback for checkpoint 2
    eventProducer.process(tp, 2); // inflight result: 0, 1, 3
    Assert.assertEquals(handler.getAckCheckpoint(TOPIC, partition), Optional.empty(),
        "Safe checkpoint should be empty");
    Assert.assertEquals(handler.getInFlightCount(TOPIC, partition), 3, "Number of inflight messages should be 3");
    Assert.assertEquals(handler.getAckMessagesPastCheckpointCount(TOPIC, partition), 2);


    // simulate callback for checkpoint 0
    eventProducer.process(tp, 0); // inflight result: 1, 3
    Assert.assertEquals(handler.getAckCheckpoint(TOPIC, partition).get(), Long.valueOf(0),
        "Safe checkpoint should be 0");
    Assert.assertEquals(handler.getInFlightCount(TOPIC, partition), 2, "Number of inflight messages should be 2");
    Assert.assertEquals(handler.getAckMessagesPastCheckpointCount(TOPIC, partition), 2);

    // simulate callback for checkpoint 1
    eventProducer.process(tp, 0); // inflight result: 3
    Assert.assertEquals(handler.getAckCheckpoint(TOPIC, partition).get(), Long.valueOf(2),
        "Safe checkpoint should be 1");
    Assert.assertEquals(handler.getInFlightCount(TOPIC, partition), 1, "Number of inflight messages should be 1");
    Assert.assertEquals(handler.getAckMessagesPastCheckpointCount(TOPIC, partition), 1);


    // send another event with checkpoint 5
    sendEvent(tp, handler, 5); // inflight result: 3, 5
    Assert.assertEquals(handler.getInFlightCount(TOPIC, partition), 2, "Number of inflight messages should be 2");
    Assert.assertEquals(handler.getAckMessagesPastCheckpointCount(TOPIC, partition), 1);


    // simulate callback for checkpoint 3
    eventProducer.process(tp, 0); // inflight result: 5
    Assert.assertEquals(handler.getAckCheckpoint(TOPIC, partition).get(), Long.valueOf(4),
        "Safe checkpoint should be 4");
    Assert.assertEquals(handler.getInFlightCount(TOPIC, partition), 1, "Number of inflight messages should be 1");
    Assert.assertEquals(handler.getAckMessagesPastCheckpointCount(TOPIC, partition), 0);

    // simulate callback for checkpoint 5
    eventProducer.process(tp, 0); // inflight result: empty
    Assert.assertEquals(handler.getAckCheckpoint(TOPIC, partition).get(), Long.valueOf(5),
        "Safe checkpoint should be 5");
    Assert.assertEquals(handler.getInFlightCount(TOPIC, partition), 0, "Number of inflight messages should be 0");
    Assert.assertEquals(handler.getAckMessagesPastCheckpointCount(TOPIC, partition), 0);

    // send another event with checkpoint 6
    sendEvent(tp, handler, 6);
    Assert.assertEquals(handler.getAckCheckpoint(TOPIC, partition).get(), Long.valueOf(5),
        "Safe checkpoint should be 5");

    // simulate callback for checkpoint 6
    eventProducer.process(tp, 0); // inflight result: empty
    Assert.assertEquals(handler.getAckCheckpoint(TOPIC, partition).get(), Long.valueOf(6),
        "Safe checkpoint should be 6");
    Assert.assertEquals(handler.getInFlightCount(TOPIC, partition), 0, "Number of inflight messages should be 0");
    Assert.assertEquals(handler.getAckMessagesPastCheckpointCount(TOPIC, partition), 0);
  }

  @Test
  public void testOutOfOrderAckForComparableOffsets() {
    RandomEventProducer eventProducer = new RandomEventProducer();
    FlushlessEventProducerHandler<Long> handler =
        new FlushlessEventProducerHandler<>(eventProducer, OFFSET_CHECKPOINT_TRACKING_STRATEGY_WITH_COMPARABLE_OFFSETS);
    testOutOfOrderAckForGivenHandler(eventProducer, handler);
  }

  @Test
  public void testOutOfOrderAckForNonComparableOffsets() {
    RandomEventProducer eventProducer = new RandomEventProducer();
    FlushlessEventProducerHandler<Long> handler =
        new FlushlessEventProducerHandler<>(eventProducer, OFFSET_CHECKPOINT_TRACKING_STRATEGY_WITH_NON_COMPARABLE_OFFSETS);
    testOutOfOrderAckForGivenHandler(eventProducer, handler);
  }

  /**
   * Helper function to test the behavior of backwards order record acking for both comparable and non comparable offsets
   * @param randomEventProducer is the event producer to send records to randomized topic partition
   * @param handler is the flushless producer handler that does offset checkpoint management
   */
  private void testBackwardsOrderAckWithGivenHandler(RandomEventProducer randomEventProducer, FlushlessEventProducerHandler<Long> handler) {
    int partition = 0;
    SourcePartition tp = new SourcePartition(TOPIC, partition);

    // Send 1000 messages to the source partition
    for (int i = 0; i < 1000; i++) {
      sendEvent(tp, handler, i);
    }

    // acknowledge the checkpoints in backward (descending order) to simulate worst case scenario
    for (int i = 999; i > 0; i--) {
      randomEventProducer.process(tp, i);
      // validate that checkpoint has to be empty because oldest message was not yet acknowledged
      Assert.assertEquals(handler.getAckCheckpoint(TOPIC, partition), Optional.empty(),
          "Safe checkpoint should be empty");
    }

    // finally process the oldest message
    randomEventProducer.process(tp, 0);
    // validate that the checkpoint was finally updated to 999
    Assert.assertEquals(handler.getAckCheckpoint(TOPIC, partition).get(), Long.valueOf(999),
        "Safe checkpoint should be 999");
  }

  @Test
  public void testBackwardsOrderAckWithComparableOffsets() {
    RandomEventProducer eventProducer = new RandomEventProducer();
    FlushlessEventProducerHandler<Long> handler =
        new FlushlessEventProducerHandler<>(eventProducer, OFFSET_CHECKPOINT_TRACKING_STRATEGY_WITH_COMPARABLE_OFFSETS);
    testBackwardsOrderAckWithGivenHandler(eventProducer, handler);
  }

  @Test
  public void testBackwardsOrderAckWithNonComparableOffsets() {
    RandomEventProducer eventProducer = new RandomEventProducer();
    FlushlessEventProducerHandler<Long> handler =
        new FlushlessEventProducerHandler<>(eventProducer, OFFSET_CHECKPOINT_TRACKING_STRATEGY_WITH_NON_COMPARABLE_OFFSETS);
    testBackwardsOrderAckWithGivenHandler(eventProducer, handler);
  }

  private void sendEvent(SourcePartition tp, FlushlessEventProducerHandler<Long> handler, long checkpoint) {
    DatastreamProducerRecord record = getDatastreamProducerRecord(checkpoint, tp.getKey(), tp.getValue());
    handler.send(record, tp.getSource(), tp.getPartition(), checkpoint, null);
  }

  private DatastreamProducerRecord getDatastreamProducerRecord(Long checkpoint, String topic, int partition) {
    BrooklinEnvelope emptyEnvelope = new BrooklinEnvelope("1", "value", new HashMap<>());
    DatastreamProducerRecordBuilder builder = new DatastreamProducerRecordBuilder();
    builder.addEvent(emptyEnvelope);
    builder.setPartition(partition);
    builder.setSourceCheckpoint(checkpoint.toString());
    builder.setEventsSourceTimestamp(System.currentTimeMillis());
    builder.setDestination(topic);
    return builder.build();
  }

  private static class RandomEventProducer implements DatastreamEventProducer {
    Map<SourcePartition, List<Pair<DatastreamProducerRecord, SendCallback>>> _queue = new HashMap<>();
    List<SourcePartition> tps = new ArrayList<>();
    private final double _exceptionProb;

    public RandomEventProducer() {
      this(0.0);
    }

    public RandomEventProducer(double exceptionProb) {
      _exceptionProb = exceptionProb;
    }

    @Override
    public synchronized void send(DatastreamProducerRecord record, SendCallback callback) {
      SourcePartition tp = new SourcePartition(TOPIC, record.getPartition().orElse(0));
      if (!tps.contains(tp)) {
        tps.add(tp);
      }
      _queue.computeIfAbsent(tp, x -> new ArrayList<>()).add(Pair.of(record, callback));
    }

    public synchronized void processOne() {
      int start = RANDOM.nextInt(tps.size());
      for (int i = 0; i < tps.size(); i++) {
        SourcePartition tp = tps.get((i + start) % tps.size());
        if (!_queue.get(tp).isEmpty()) {
          Pair<DatastreamProducerRecord, SendCallback> pair = _queue.get(tp).remove(0);
          DatastreamProducerRecord record = pair.getKey();
          SendCallback callback = pair.getValue();

          DatastreamRecordMetadata metadata =
              new DatastreamRecordMetadata(record.getCheckpoint(), TOPIC, record.getPartition().orElse(0));
          Exception exception = null;
          if (RANDOM.nextDouble() < _exceptionProb) {
            exception = new RuntimeException("Simulating Flakiness sending messages");
          }
          callback.onCompletion(metadata, exception);
          return;
        }
      }
    }

    public synchronized void process(SourcePartition sourcePartition, int queueIndex) {
      List<Pair<DatastreamProducerRecord, SendCallback>> messages = _queue.get(sourcePartition);
      if (messages == null) {
        throw new IllegalArgumentException(
            "There are no messages in the queue for source partition " + sourcePartition);
      }

      if (queueIndex >= messages.size()) {
        throw new IllegalArgumentException(
            "Cannot remove message at index " + queueIndex + " for source partition " + sourcePartition
                + " because messages count is " + messages.size());
      }

      Pair<DatastreamProducerRecord, SendCallback> pair = messages.remove(queueIndex);
      DatastreamProducerRecord record = pair.getKey();
      SendCallback callback = pair.getValue();

      DatastreamRecordMetadata metadata =
          new DatastreamRecordMetadata(record.getCheckpoint(), TOPIC, record.getPartition().orElse(0));
      callback.onCompletion(metadata, null);
    }

    @Override
    public synchronized void flush() {
      while (_queue.values().stream().anyMatch(l -> !l.isEmpty())) {
        processOne();
      }
    }

    public long minCheckpoint() {
      return _queue.values()
          .stream()
          .flatMap(Collection::stream)
          .map(Pair::getKey)
          .map(DatastreamProducerRecord::getCheckpoint)
          .mapToLong(Long::valueOf)
          .min()
          .getAsLong();
    }
  }
}
