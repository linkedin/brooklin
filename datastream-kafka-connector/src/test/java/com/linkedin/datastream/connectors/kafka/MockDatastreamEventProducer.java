package com.linkedin.datastream.connectors.kafka;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Predicate;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.datastream.common.DatastreamRuntimeException;
import com.linkedin.datastream.server.DatastreamEventProducer;
import com.linkedin.datastream.server.DatastreamProducerRecord;
import com.linkedin.datastream.server.api.transport.DatastreamRecordMetadata;
import com.linkedin.datastream.server.api.transport.SendCallback;


public class MockDatastreamEventProducer implements DatastreamEventProducer {

  private static final Logger LOG = LoggerFactory.getLogger(MockDatastreamEventProducer.class);
  private final List<DatastreamProducerRecord> events = Collections.synchronizedList(new ArrayList<>());
  private int numFlushes = 0;
  private ExecutorService _executorService = Executors.newFixedThreadPool(1);
  private Duration _callbackThrottleDuration;
  private Predicate<DatastreamProducerRecord> _sendFailCondition;

  private static final Duration DEFAULT_CALLBACK_THROTTLE_DURATION = Duration.ofMillis(0);
  private static final Predicate<DatastreamProducerRecord> DEFAULT_SEND_FAIL_CONDITION = (r) -> false;

  public MockDatastreamEventProducer() {
    this(DEFAULT_CALLBACK_THROTTLE_DURATION, DEFAULT_SEND_FAIL_CONDITION);
  }

  public MockDatastreamEventProducer(Predicate<DatastreamProducerRecord> sendFailCondition) {
    this(DEFAULT_CALLBACK_THROTTLE_DURATION, sendFailCondition);
  }

  public MockDatastreamEventProducer(Duration callbackThrottleDuration) {
    this(callbackThrottleDuration, DEFAULT_SEND_FAIL_CONDITION);
  }

  public MockDatastreamEventProducer(Duration callbackThrottleDuration, Predicate<DatastreamProducerRecord> sendFailCondition) {
    _callbackThrottleDuration = callbackThrottleDuration;
    _sendFailCondition = sendFailCondition;
  }

  @Override
  public void send(DatastreamProducerRecord event, SendCallback callback) {
    if (_sendFailCondition.test(event)) {
      throw new DatastreamRuntimeException("Random exception");
    }

    // potentially throttle send callbacks by sleeping for specified duration
    Optional.ofNullable(_callbackThrottleDuration).ifPresent(d -> {
      _executorService.submit(() -> {
        try {
          Thread.sleep(d.toMillis());
          events.add(event);
          LOG.info("sent event {} , total events {}", event, events.size());
          DatastreamRecordMetadata md = new DatastreamRecordMetadata(event.getCheckpoint(), "mock topic", 666);
          callback.onCompletion(md, null);
        } catch (InterruptedException e) {
          LOG.error("Sleep was interrupted while throttling send callback");
          throw new DatastreamRuntimeException("Sleep was interrupted", e);
        }
      });
    });
  }

  @Override
  public void flush() {
    numFlushes++;
  }

  public List<DatastreamProducerRecord> getEvents() {
    return events;
  }

  public int getNumFlushes() {
    return numFlushes;
  }

  public void updateSendFailCondition(Predicate<DatastreamProducerRecord> sendFailCondition) {
    _sendFailCondition = sendFailCondition;
  }
}
