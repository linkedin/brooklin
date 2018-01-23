package com.linkedin.datastream.server;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.linkedin.datastream.common.BrooklinEnvelope;
import com.linkedin.datastream.server.api.transport.DatastreamRecordMetadata;
import com.linkedin.datastream.server.api.transport.SendCallback;

import static com.linkedin.datastream.server.FlushlessEventProducerHandler.DestinationPartition;


public class TestFlushlessEventProducerHandler {
  private static final Long BIG_CHECKPOINT = Long.MAX_VALUE;
  private static Random _rnd = new Random();
  private static final String TOPIC = "MyTopic";

  @Test
  public void testSingleRecord() throws Exception {
    RandomEventProducer eventProducer = new RandomEventProducer();
    FlushlessEventProducerHandler<Long> handler = new FlushlessEventProducerHandler<>(eventProducer);

    long checkpoint = 1;
    DatastreamProducerRecord record = getDatastreamProducerRecord(checkpoint, TOPIC, 1);

    Assert.assertEquals(handler.getAckCheckpoint(BIG_CHECKPOINT, Comparator.naturalOrder()), BIG_CHECKPOINT);
    handler.send(record, checkpoint);
    Assert.assertNull(handler.getAckCheckpoint(BIG_CHECKPOINT, Comparator.naturalOrder()));
    Assert.assertEquals(handler.getInFlightCount(TOPIC, 1), 1);
    Assert.assertEquals(handler.getAckCheckpoint(TOPIC, 1), null);
    eventProducer.flush();
    Assert.assertEquals(handler.getAckCheckpoint(BIG_CHECKPOINT, Comparator.naturalOrder()), BIG_CHECKPOINT);
    Assert.assertEquals(handler.getInFlightCount(TOPIC, 1), 0);
    Assert.assertEquals(handler.getAckCheckpoint(TOPIC, 1), new Long(checkpoint));
  }

  @Test
  public void testMultipleSends() throws Exception {
    RandomEventProducer eventProducer = new RandomEventProducer();
    FlushlessEventProducerHandler<Long> handler = new FlushlessEventProducerHandler<>(eventProducer);

    // Send 100 messages
    for (int i = 0; i < 1000; i++) {
      DestinationPartition tp = new DestinationPartition(TOPIC, _rnd.nextInt(10));
      sendEvent(tp, handler, i / 2);
    }

    for (int i = 0; i < 999; i++) {
      eventProducer.processOne();
      long minOffsetPending = eventProducer.minCheckpoint();
      Long checkpoint = handler.getAckCheckpoint(BIG_CHECKPOINT, Comparator.naturalOrder());
      long ackOffset = checkpoint == null ? -1 : checkpoint;

      Assert.assertTrue(ackOffset < minOffsetPending,
          "Not true that " + ackOffset + " is less than " + minOffsetPending);
    }
    eventProducer.processOne();

    for (int par = 0; par < 10; par++) {
      Assert.assertEquals(handler.getInFlightCount(TOPIC, par), 0);
    }

    Assert.assertEquals(handler.getAckCheckpoint(BIG_CHECKPOINT, Comparator.naturalOrder()), BIG_CHECKPOINT);

    Assert.assertEquals(handler.getAckCheckpoint(BIG_CHECKPOINT, Comparator.naturalOrder()), BIG_CHECKPOINT);
  }

  private void sendEvent(DestinationPartition tp, FlushlessEventProducerHandler<Long> handler, long checkpoint) {
    DatastreamProducerRecord record = getDatastreamProducerRecord(checkpoint, tp.getKey(), tp.getValue());
    handler.send(record, checkpoint);
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
    Map<DestinationPartition, List<Pair<DatastreamProducerRecord, SendCallback>>> _queue = new HashMap<>();
    List<DestinationPartition> tps = new ArrayList<>();
    private double _exceptionProb;

    public RandomEventProducer() {
      this(0.0);
    }

    public RandomEventProducer(double exceptionProb) {
      _exceptionProb = exceptionProb;
    }

    @Override
    public synchronized void send(DatastreamProducerRecord record, SendCallback callback) {
      DestinationPartition tp = new DestinationPartition(TOPIC, record.getPartition().orElse(0));
      if (!tps.contains(tp)) {
        tps.add(tp);
      }
      _queue.computeIfAbsent(tp, x -> new ArrayList<>()).add(Pair.of(record, callback));
    }

    public synchronized void processOne() {
      int start = _rnd.nextInt(tps.size());
      for (int i = 0; i < tps.size(); i++) {
        DestinationPartition tp = tps.get((i + start) % tps.size());
        if (!_queue.get(tp).isEmpty()) {
          Pair<DatastreamProducerRecord, SendCallback> pair = _queue.get(tp).remove(0);
          DatastreamProducerRecord record = pair.getKey();
          SendCallback callback = pair.getValue();

          DatastreamRecordMetadata metadata =
              new DatastreamRecordMetadata(record.getCheckpoint(), TOPIC, record.getPartition().orElse(0));
          Exception exception = null;
          if (_rnd.nextDouble() < _exceptionProb) {
            exception = new RuntimeException("Simulating Flakiness sending messages");
          }
          callback.onCompletion(metadata, exception);
          return;
        }
      }
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
